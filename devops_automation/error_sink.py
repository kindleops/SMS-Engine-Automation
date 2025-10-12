import traceback
from .devops_logger import log_devops


def report(context: str, err: Exception, severity="Error"):
    try:
        log_devops("Error", context, {"trace": traceback.format_exc()[:9000]}, status="FAIL", severity=severity)
    except Exception:
        traceback.print_exc()
