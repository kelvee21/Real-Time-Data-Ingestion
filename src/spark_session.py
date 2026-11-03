import logging
import os
import sys
import warnings
from pathlib import Path

from pyspark.sql import SparkSession

from src.config import JDBC_PACKAGE

log = logging.getLogger(__name__)


def _saved_user_env(name: str) -> str | None:
    """Read a user env var straight from the registry (windows only)."""
    if sys.platform != "win32":
        return None
    import winreg

    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as key:
            value, _ = winreg.QueryValueEx(key, name)
    except OSError:
        return None
    return str(value)


def ensure_hadoop_home() -> None:
    """On windows, point the jvm at winutils.exe before spark starts.

    the checkpoint folder is written through hadoop's file system code, which
    needs winutils.exe + hadoop.dll on windows. setx only reaches programs
    opened after it, so a vs code that was already open (and the jupyter
    kernel it starts) never sees HADOOP_HOME. so: real env var first (.env is
    loaded by src.config), then the value saved in the registry. the bin
    folder goes on PATH too, that is where hadoop.dll is.
    """
    if sys.platform != "win32":
        return
    home = os.getenv("HADOOP_HOME") or _saved_user_env("HADOOP_HOME")
    if not home or not (Path(home) / "bin" / "winutils.exe").exists():
        warnings.warn(
            "HADOOP_HOME is not set or has no bin\\winutils.exe, "
            "the checkpoint writes will fail (see study/01)",
            stacklevel=2,
        )
        return
    os.environ["HADOOP_HOME"] = home
    bin_dir = str(Path(home) / "bin")
    if bin_dir.lower() not in os.environ.get("PATH", "").lower():
        os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")


def get_spark(app_name: str = "ecommerce-events-to-postgres") -> SparkSession:
    """Build (or reuse) the one SparkSession the job, the notebook and the tests share."""
    # on windows the workers can pick up a different python than the driver
    # and then crash on import. pin both to the python running this file.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    # has to happen before getOrCreate, the jvm copies the env when it starts
    ensure_hadoop_home()
    master = os.getenv("SPARK_MASTER", "local[*]")
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        # the postgres jdbc driver. spark pulls it from maven the first time
        # and keeps it in ~/.ivy2, so only the first run needs internet.
        .config("spark.jars.packages", JDBC_PACKAGE)
        # default 200 shuffle partitions is way too many for 100 row batches,
        # dropDuplicates would run 200 tiny tasks every trigger.
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
        # the generator writes utc times, so spark reads them as utc too
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    log.info("spark %s on %s, ui at %s", spark.version, master, spark.sparkContext.uiWebUrl)
    return spark
