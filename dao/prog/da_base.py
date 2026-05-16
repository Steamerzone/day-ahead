import datetime
import sys
import os
import fnmatch
import time
import threading
import pytz
import warnings
import logging
from json import loads
from dataclasses import dataclass
from pathlib import Path
from typing import Union, Dict, Any

import pandas as pd
from sqlalchemy import Table, select, func, and_
from requests import get, RequestException

import hassapi as hass
from hassapi.models import StateList

from dao.prog.utils import get_tibber_data, error_handling
from dao.prog.version import __version__
from dao.prog.config.loader import ConfigurationLoader
from dao.lib.db_connections import make_db_da, make_db_ha
from dao.lib.da_meteo import Meteo
from dao.lib.da_prices import DaPrices
from dao.prog.utils import interpolate


@dataclass
class HAContext:
    """Runtime values fetched from Home Assistant on start-up."""
    latitude: float
    longitude: float
    time_zone: str
    country: str


class NotificationHandler(logging.Handler):
    def __init__(self, _hass: hass.Hass, _entity: str = None):
        super().__init__()
        self.hass = _hass
        self.entity = _entity
        self.count = 0

    def emit(self, record):
        if self.entity and record.levelno >= logging.WARNING and self.count == 0:
            if record.levelno >= logging.ERROR:
                self.count += 1
            msg = self.format(record).partition("\n")[0]
            try:
                self.hass.set_value(self.entity, msg)
            except Exception:
                self.handleError(record)


class DaBase(hass.Hass):
    _config = None
    _loader = None
    _init_lock = threading.Lock()

    def __init__(self, file_name: str = None):
        self.file_name = file_name
        
        # 1. Align System Paths
        path = os.getcwd()
        new_path = "/".join(path.split("/")[:-2])
        if new_path not in sys.path:
            sys.path.append(new_path)
        
        self.make_data_path()
        self.debug = False
        self.tasks = self.generate_tasks()
        self.log_level = logging.INFO
        self.notification_entity = None
        self.ha_context: HAContext | None = None

        # 2. Synchronized Configuration Processing
        with DaBase._init_lock:
            if DaBase._config is None:
                try:
                    config_path = Path(self.file_name) if self.file_name else Path("../data/options.json")
                    DaBase._loader = ConfigurationLoader(config_path)
                    DaBase._config = DaBase._loader.load_and_validate()
                except Exception as e:
                    logging.critical(f"Fatal initialization error loading configuration: {e}")
                    raise RuntimeError("Application cannot start without valid configuration.") from e

        self.config = DaBase._config
        self.loader = DaBase._loader

        # 3. Database Initialization Validation
        self.db_da = make_db_da(self.config, self.loader.secrets)
        self.db_ha = make_db_ha(self.config, self.loader.secrets)
        if self.db_da is None or self.db_ha is None:
            raise RuntimeError("Fatal database connection failure during system startup.")

        # 4. Logger Setup
        log_level_str = self.config.logging_level or "info"
        numeric_level = getattr(logging, log_level_str.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError(f"Invalid log level specified: {log_level_str}")
        self.log_level = numeric_level
        
        logging.addLevelName(logging.WARNING, "waarschuwing")
        logging.addLevelName(logging.ERROR, "fout")
        logging.addLevelName(logging.CRITICAL, "kritiek")
        logging.getLogger().setLevel(self.log_level)

        # 5. Extract Network Endpoints
        ha_cfg = self.config.homeassistant
        self.protocol_api = ha_cfg.protocol_api
        self.ip_address = ha_cfg.ip_address
        self.ip_port = ha_cfg.ip_port
        
        if self.ip_port is None:
            self.hassurl = f"{self.protocol_api}://{self.ip_address}/core/"
        else:
            self.hassurl = f"{self.protocol_api}://{self.ip_address}:{self.ip_port}/"

        _tok = ha_cfg.hasstoken
        self.hasstoken = _tok.resolve(self.loader.secrets) if _tok else os.environ.get("SUPERVISOR_TOKEN")

        # 6. Initialize Parent Class
        super().__init__(hassurl=self.hassurl, token=self.hasstoken, timeout=10)

        # 7. Sync Home Assistant Context Boundaries
        try:
            headers = {
                "Authorization": f"Bearer {self.hasstoken}",
                "content-type": "application/json",
            }
            resp = get(f"{self.hassurl}api/config", headers=headers, timeout=10)
            resp.raise_for_status()
            resp_dict = resp.json()
        except (RequestException, ValueError) as e:
            raise RuntimeError(f"Failed parsing connection payload metadata from Home Assistant: {e}") from e

        self.ha_context = HAContext(
            latitude=resp_dict["latitude"],
            longitude=resp_dict["longitude"],
            time_zone=resp_dict["time_zone"],
            country=resp_dict.get("country") or "NL",
        )
        self.time_zone = self.ha_context.time_zone
        
        # 8. Dependency Orchestration Instantiations
        self.meteo = Meteo(
            self.config, self.db_da,
            latitude=self.ha_context.latitude,
            longitude=self.ha_context.longitude,
            secrets=self.loader.secrets,
        )
        if self.ha_context.country in ("NL", "BE"):
            self.knmi_station = self.meteo.which_station()
            
        self.solar = self.config.solar
        self.interval = self.config.interval
        self.interval_s = 3600 if self.interval == "1hour" else 900

        self.prices = DaPrices(self.config, self.db_da, country=self.ha_context.country, secrets=self.loader.secrets)
        self.prices_options = self.config.prices
        
        if self.prices_options:
            self.taxes_l_def = self.prices_options.energy_taxes_consumption
            self.ol_l_def = self.prices_options.cost_supplier_consumption
            self.taxes_t_def = self.prices_options.energy_taxes_production
            self.ol_t_def = self.prices_options.cost_supplier_production
            self.btw_l_def = self.prices_options.vat_consumption
            self.btw_t_def = self.prices_options.vat_production or self.btw_l_def
            self.salderen = self.prices_options.tax_refund
        else:
            self.taxes_l_def = self.ol_l_def = self.taxes_t_def = self.ol_t_def = None
            self.btw_l_def = self.btw_t_def = None
            self.salderen = True

        self.history_options = self.config.history
        self.strategy = self.config.strategy.resolve(lambda eid: self.get_state(eid).state)
        self.tibber_options = self.config.tibber
        
        notif = self.config.notifications
        self.notification_entity = notif.notification_entity
        self.notification_opstarten = notif.opstarten
        self.notification_berekening = notif.berekening
        self.last_activity_entity = notif.last_activity_entity
        
        self.set_last_activity()
        self.graphics_options = self.config.graphics
        self.db_da.log_pool_status()
        warnings.simplefilter("ignore", ResourceWarning)

    def set_value(self, entity_id: str, value: Union[int, float, str]) -> StateList:
        try:
            result = super().set_value(entity_id, value)
            state = self.get_state(entity_id).state
            if isinstance(value, (int, float)):
                if round(float(state), 5) != round(float(value), 5):
                    raise ValueError(f"State verification failed for {entity_id}. Sent: {value}, Got: {state}")
            elif state != value:
                raise ValueError(f"State verification failed for {entity_id}. Sent: {value}, Got: {state}")
            return result
        except Exception:
            logging.error(f"Fout bij schrijven naar {entity_id}, waarde {value}")
            raise

    @staticmethod
    def generate_tasks() -> Dict[str, Dict[str, Any]]:
        return {
            "calc_optimum_met_debug": {
                "name": "Optimaliseringsberekening met debug",
                "cmd": ["python3", "../prog/day_ahead.py", "debug", "calc"],
                "object": "DaCalc",
                "function": "calc_optimum_met_debug",
                "file_name": "calc_debug",
            },
            "calc_optimum": {
                "name": "Optimaliseringsberekening zonder debug",
                "cmd": ["python3", "../prog/day_ahead.py", "calc"],
                "object": "DaBase",
                "function": "calc_optimum",
                "file_name": "calc",
            },
            "tibber": {
                "name": "Verbruiksgegevens bij Tibber ophalen",
                "cmd": ["python3", "../prog/day_ahead.py", "tibber"],
                "function": "get_tibber_data",
                "file_name": "tibber",
            },
            "meteo": {
                "name": "Meteoprognoses ophalen",
                "cmd": ["python3", "day_ahead.py", "meteo"],
                "function": "get_meteo_data",
                "file_name": "meteo",
            },
            "prices": {
                "name": "Day ahead prijzen ophalen",
                "cmd": ["python3", "../prog/day_ahead.py", "prices"],
                "function": "get_day_ahead_prices",
                "file_name": "prices",
            },
            "calc_baseloads": {
                "name": "Bereken de baseloads",
                "cmd": ["python3", "../prog/day_ahead.py", "calc_baseloads"],
                "function": "calc_baseloads",
                "file_name": "baseloads",
            },
            "clean": {
                "name": "Bestanden opschonen",
                "cmd": ["python3", "../prog/day_ahead.py", "clean_data"],
                "function": "clean_data",
                "file_name": "clean",
            },
            "train_ml_predictions": {
                "name": "ML modellen trainen",
                "cmd": ["python3", "../prog/day_ahead.py", "train"],
                "function": "train_ml_predictions",
                "file_name": "train",
            },
            "consolidate": {
                "name": "Verbruik/productie consolideren",
                "cmd": ["python3", "../prog/day_ahead.py", "consolidate"],
                "function": "consolidate_data",
                "file_name": "consolidate",
            },
        }

    def start_logging(self):
        logging.debug(f"python pad:{sys.path}")
        logging.info(f"Day Ahead Optimalisering versie: {__version__}")
        logging.info(f"Day Ahead Optimalisering gestart op: {datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")
        if self.config and self.ha_context:
            logging.debug(f"Locatie: latitude {self.ha_context.latitude} longitude: {self.ha_context.longitude}")

    @staticmethod
    def make_data_path():
        if not os.path.lexists("../data"):
            os.symlink("/config/dao_data", "../data")

    def set_last_activity(self):
        if self.last_activity_entity:
            self.call_service(
                "set_datetime",
                entity_id=self.last_activity_entity,
                datetime=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )

    def get_meteo_data(self, show_graph: bool = False):
        self.meteo.get_meteo_data(show_graph)

    @staticmethod
    def get_tibber_data():
        get_tibber_data()

    @staticmethod
    def consolidate_data():
        from da_report import Report
        report = Report()
        start_dt = None
        if len(sys.argv) > 2:
            try:
                start_dt = datetime.datetime.strptime(sys.argv[2], "%Y-%m-%d")
            except Exception as ex:
                error_handling(ex)
                return
        report.consolidate_data(start_dt)

    def get_day_ahead_prices(self):
        source = self.prices_options.source_day_ahead if self.prices_options else "nordpool"
        self.prices.get_prices(source)

    def save_df(self, tablename: str, tijd: list, df: pd.DataFrame):
        df = df.reset_index(drop=True)
        columns = df.columns.values.tolist()[1:]
        tz = pytz.timezone(self.time_zone)
        
        rows = []
        for index in range(min(len(tijd), len(df))):
            dt = pd.to_datetime(tijd[index])
            if dt.tzinfo is None:
                dt = tz.localize(dt)
            utc_timestamp = int(dt.timestamp())
            for c in columns:
                rows.append({"time": str(utc_timestamp), "code": c, "value": float(df.loc[index, c])})
                
        df_db = pd.DataFrame(rows, columns=["time", "code", "value"])
        logging.debug(f"Save calculated data:\n{df_db.to_string()}")
        self.db_da.savedata(df_db, tablename=tablename)

    @staticmethod
    def get_calculated_baseload(weekday: int) -> list:
        in_file = f"../data/baseload/baseload_{weekday}.json"
        with open(in_file, "r") as f:
            return load(f)

    def calc_prod_solar(self, solar_opt: Any, act_time: int, act_gr: float, hour_fraction: float) -> float:
        if hasattr(solar_opt, 'strings') and solar_opt.strings:
            prod = 0.0
            for string in solar_opt.strings:
                prod += (
                    self.meteo.calc_solar_rad(string, act_time, act_gr)
                    * string.yield_factor
                    * hour_fraction
                )
        else:
            prod = (
                self.meteo.calc_solar_rad(solar_opt, act_time, act_gr)
                * solar_opt.yield_factor
                * hour_fraction
            )
        
        if getattr(solar_opt, 'max_power', None) is not None:
            prod = min(prod, solar_opt.max_power)
        return prod

    def calc_da_avg(self) -> float:
        values_table = Table("values", self.db_da.metadata, autoload_with=self.db_da.engine)
        variabel_table = Table("variabel", self.db_da.metadata, autoload_with=self.db_da.engine)

        inner_query = (
            select(
                values_table.c.time,
                values_table.c.value,
                self.db_da.from_unixtime(values_table.c.time).label("begin"),
            )
            .where(
                and_(
                    variabel_table.c.code == "da",
                    values_table.c.variabel == variabel_table.c.id,
                )
            )
            .order_by(values_table.c.time.desc())
            .limit(24)
            .alias("t1")
        )

        outer_query = select(func.avg(inner_query.c.value).label("avg_da"))

        with self.db_da.engine.connect() as connection:
            logging.debug(f"inner query p_avg: {inner_query.compile(connection)}")
            logging.debug(f"outer query p_avg: {outer_query.compile(connection)}")
            result = connection.execute(outer_query)
            return result.scalar()

    @staticmethod
    def _get_option(key: str, options, default=None):
        if options is None:
            return default
        if isinstance(options, dict):
            return options.get(key, default)
        snake_key = key.replace(' ', '_').replace('-', '_')
        val = getattr(options, snake_key, None)
        if val is None:
            val = getattr(options, key, None)
        return val if val is not None else default

    def set_entity_value(self, entity_key: str, options, value: int | float | str):
        entity_id = self._get_option(entity_key, options)
        if entity_id is not None:
            self.set_value(entity_id, value)

    def set_entity_option(self, entity_key: str, options, value: int | float | str):
        entity_id = self._get_option(entity_key, options)
        if entity_id is not None:
            self.select_option(entity_id, value)

    def set_entity_state(self, entity_key: str, options, value: int | float | str):
        entity_id = self._get_option(entity_key, options)
        if entity_id is not None:
            self.set_state(entity_id, value)

    def get_entity_state(self, entity_key: str, options) -> int | float | str | None:
        entity_id = self._get_option(entity_key, options)
        return self.get_state(entity_id).state if entity_id is not None else None

    def clean_data(self):
        def clean_folder(folder: str, pattern: str):
            current_time = time.time()
            seconds_in_day = 24 * 60 * 60
            logging.info(f"Start removing files in {folder} with pattern {pattern}")
            
            resolved_folder = Path(folder).resolve()
            if not resolved_folder.exists():
                return

            for file_path in resolved_folder.iterdir():
                if file_path.is_file() and fnmatch.fnmatch(file_path.name, pattern):
                    creation_time = file_path.stat().st_ctime
                    save_days = self.history_options.save_days
                    if (current_time - creation_time) >= save_days * seconds_in_day:
                        try:
                            file_path.unlink()
                            logging.info(f"{file_path.name} removed")
                        except Exception as e:
                            logging.error(f"Failed to delete {file_path.name}: {e}")

        clean_folder("../data/log", "*.log")
        clean_folder("../data/log", "dashboard.log.*")
        clean_folder("../data/images", "*.png")

    def calc_optimum_met_debug(self):
        from day_ahead import DaCalc
        dacalc = DaCalc(self.file_name)
        dacalc.debug = True
        dacalc.calc_optimum()

    def calc_optimum(self):
        from day_ahead import DaCalc
        dacalc = DaCalc(self.file_name)
        dacalc.debug = False
        dacalc.calc_optimum()

    @staticmethod
    def calc_baseloads():
        from da_report import Report
        report = Report()
        report.calc_save_baseloads()

    def calc_solar_predictions(
        self,
        solar_option: Any,
        vanaf: datetime.datetime,
        tot: datetime.datetime,
        interval: str = None,
        _ml_prediction: bool = None,
    ) -> pd.DataFrame:
        from dao.prog.solar_predictor import SolarPredictor

        ml_prediction = solar_option.ml_prediction if _ml_prediction is None else _ml_prediction
        interval = interval or self.interval
        interval_s = 900 if interval == "15min" else 3600
        solar_name = solar_option.name.replace(" ", "_").replace("-", "_")

        if ml_prediction:
            solar_predictor = SolarPredictor()
            try:
                solar_prog = solar_predictor.predict_solar_device(solar_option, vanaf, tot)
                if solar_prog.isnull().any().any():
                    logging.warning(f"NaN-waarden aangetroffen in voorspelling van {solar_name}. Deze zijn op '0' gezet")
                    solar_prog.fillna(0, inplace=True)
            except FileNotFoundError as ex:
                logging.warning(ex)
                logging.info(f"Voor {solar_option.name} is geen model en dus wordt DAO-predictor gebruikt")

                result = self.calc_solar_predictions(
                    solar_option, vanaf, tot, interval=interval, _ml_prediction=False
                )
                if _ml_prediction:
                    result["prediction"] = pd.NA
                return result
            
            solar_prog["tijd"] = pd.to_datetime(solar_prog["date_time"])
            if interval == "15min":
                solar_prog = interpolate(solar_prog, "prediction", quantity=True)
            
            # Efficient vectorized drop instead of step-by-step looping
            solar_prog = solar_prog[solar_prog["tijd"].dt.tz_localize(None) >= vanaf].reset_index(drop=True)
        else:
            solar_prog = pd.DataFrame(columns=["tijd", "prediction"])
            start_ts = datetime.datetime(year=vanaf.year, month=vanaf.month, day=vanaf.day, hour=vanaf.hour).timestamp()
            prog_data = self.db_da.get_prognose_data(start=start_ts, end=tot.timestamp(), interval=interval)
            
            prog_data.index = pd.to_datetime(prog_data["tijd"])
            prog_data = prog_data[prog_data["tijd"] >= vanaf]
            
            rows = []
            h_frac = interval_s / 3600
            for row in prog_data.itertuples():
                prod = self.calc_prod_solar(solar_option, row.time, row.glob_rad, h_frac)
                rows.append({"tijd": row.tijd, "prediction": round(prod, 3)})
            
            solar_prog = pd.DataFrame(rows, columns=["tijd", "prediction"])
            
        solar_prog.reset_index(drop=True, inplace=True)
        return solar_prog

    @staticmethod
    def train_ml_predictions():
        from dao.prog.solar_predictor import SolarPredictor
        solar_predictor = SolarPredictor()
        solar_predictor.run_train()

    def run_task_function(self, task: str, logfile: bool = True):
        if task not in self.tasks:
            logging.error(f"Task context target error: {task} is not registered.")
            return
            
        run_task = self.tasks[task]
        logger = logging.getLogger()
        
        # Capture current environment handlers to safely restore them post-execution
        original_handlers = logger.handlers[:]
        active_handlers = []

        formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

        try:
            if logfile:
                for handler in original_handlers:
                    logger.removeHandler(handler)
                    
                file_name = f"../data/log/{run_task['file_name']}_{datetime.datetime.now().strftime('%Y-%m-%d__%H:%M')}.log"
                file_handler = logging.FileHandler(file_name)
                file_handler.setLevel(self.log_level)
                file_handler.setFormatter(formatter)
                logger.addHandler(file_handler)
                active_handlers.append(file_handler)

                stream_handler = logging.StreamHandler(sys.stdout)
                stream_handler.setFormatter(formatter)
                stream_handler.setLevel(self.log_level)
                logger.addHandler(stream_handler)
                active_handlers.append(stream_handler)

            if self.notification_entity is not None:
                notification_handler = NotificationHandler(_hass=self, _entity=self.notification_entity)
                notification_handler.setFormatter(formatter)
                logger.addHandler(notification_handler)
                active_handlers.append(notification_handler)

            self.start_logging()
            logging.info(f"Day Ahead Optimalisatie gestart: {datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')} taak: {run_task['function']}")
            
            self.db_da.log_pool_status()
            getattr(self, run_task["function"])()
            self.set_last_activity()
            self.db_da.log_pool_status()

        except Exception:
            logging.exception("Er is een fout opgetreden binnen de runtime execution framework loop")
            raise
        finally:
            # Safely flush and tear down temporary runtime handlers
            for handler in active_handlers:
                try:
                    handler.flush()
                    handler.close()
                except Exception:
                    pass
                logger.removeHandler(handler)
            
            # Reconstruct core logging chain state configuration
            for handler in original_handlers:
                logger.addHandler(handler)

    def run_task_cmd(self, task: str):
        if task not in self.tasks:
            logging.error(f"Onbekende taak: {task}")
            return
        run_task = self.tasks[task]
        cmd = run_task["cmd"]
        
        proc = run(cmd, stdout=PIPE, stderr=PIPE)
        log_content = proc.stdout.decode() + proc.stderr.decode()
        
        filename = f"../data/log/{run_task['file_name']}_{datetime.datetime.now().strftime('%Y-%m-%d__%H:%M:%S')}.log"
        with open(filename, "w") as f:
            f.write(log_content)
