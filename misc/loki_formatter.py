import traceback

# TODO: ADD COMMENTS AND DOCSTRINGS.

class LokiFormatter:
    """
    A custom formatter for the Loki logging system, needed by the native library.
    This formatter is used to format the log records into a format that can be
    sent to the Loki logging system.
    """

    FORMAT = "[<level>{time:YYYY-MM-DD HH:mm:ss.SSS!UTC} | {extra[ID]}/{module}.{function} @ L{line}</level>] {message}"

    @classmethod
    def format(cls, record: dict):
        """
        Format the log record into a format that can be sent to the Loki logging system.
        Not intended to be used as a standalone formatter, but rather as a part of the
        Loki internals.
        Inputs: (dict) The log record provided by Loguru in its original JSON form.
        Output: (dict) The formatted log record.
        """
        formatted = {
            "message": record.get("message"),
            "timestamp": record.get("time").timestamp(),
            "process": record.get("process").id,
            "thread": record.get("thread").id,
            "function": record.get("function"),
            "module": record.get("module"),
            "name": record.get("name"),
            "level": record.get("level").name,
            "line": record.get("line")
        }

        if record.get("extra"):
            if record.get("extra").get("extra"):
                formatted |= record.get("extra").get("extra")
            else:
                formatted |= record.get("extra")

        if record.get("level").name == "ERROR":
            formatted["file"] = record.get("file").name
            formatted["path"] = record.get("file").path
            formatted["line"] = record.get("line")

            if record.get("exception"):
                exc_type, exc_value, exc_traceback = record.get("exception")
                formatted_traceback = traceback.format_exception(
                    exc_type, exc_value, exc_traceback
                )
                formatted["stacktrace"] = "".join(formatted_traceback)

        return formatted