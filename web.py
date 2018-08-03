import logging

import motor
import tornado.web
import tornado.gen
import tornado.ioloop
import tornado.options
import tornado.log


#################### Custom Errors ####################
BAD_REQUEST_BODY = 4000
INSUFFICIENT_DATA = 4001
INCONSISTENT_DATA = 4002
class BaseException(Exception):
    def __init__(self, status_code=None, error_code=None, error_message=None, additional_payload=None, level=None, log_exception=False):
        super().__init__()
        self.additional_payload = additional_payload or {}
        self.status_code = status_code or 400
        self.error_code = error_code
        self.error_message = error_message or "Internal Server Error"
        self.level = level or logging.ERROR
        self.log_exception = log_exception # if true the stacktrace will be printed

    def __str__(self):
        return "{0}: {1}".format(self.status_code, self.error_message)

    @property
    def response(self):
        resp = dict(status_code=self.status_code, error_code=self.error_code, error_message=self.error_message)
        resp.update(self.additional_payload)
        return resp


class ArgumentException(BaseException):
    MISSING_ARGUMENT = 1000
    INVALID_ARGUMENT = 1001

    def __init__(self, key, error_code, error_message, log_exception=False):
        super().__init__(status_code=400, error_code=error_code, error_message=error_message, additional_payload=dict(key=key), level=logging.INFO, log_exception=log_exception)
        self.key = key


class JsonArgumentException(BaseException):

    def __init__(self, log_exception=False):
        super().__init__(status_code=400, error_code=BAD_REQUEST_BODY, error_message="Invalid Json Body", level=logging.INFO, log_exception=log_exception)
#################### Base Handler ####################
class BaseHandler(tornado.web.RequestHandler):

    @property
    def is_json(self):
        if not hasattr(self, "_is_json"):
            self._is_json = self.request.headers.get("Content-Type", "") == "application/json"
        return self._is_json

    @property
    def json_body(self):
        if not hasattr(self, "_json_body"):
            try:
                self._json_body = json.loads(self.request.body.decode("utf-8"))
            except ValueError as e:
                raise JsonArgumentException()
        return self._json_body

    def has_flag(self, flag):
        try:
            self.get_query_argument(flag)
        except tornado.web.MissingArgumentError as e:
            return False
        return True

    def get_ip(self):
        if self.request.headers.get("Remote-Ip", None) is not None: # custom header
            return self.request.headers.get("Remote-Ip")
        elif self.request.headers.get("X-Real-Ip", None) is not None:
            return self.request.headers.get("X-Real-Ip")
        elif self.request.headers.get("X-Forwarded-For", None) is not None:
            return self.request.headers.get("X-Forwarded-For")
        else:
            return self.request.remote_ip
    #################### Output #####################################
    def write_logger(self, cls, exception, tb):
        if isinstance(exception, BaseException):
            info = {
                    "status": exception.response.get('status_code'),
                    }
            error_logger = logging.LoggerAdapter(logging.getLogger("tornado.application"), info)
            error_logger.error(exception.response)
        else:
            pass  # todo

    def write_json(self, obj, status_code=200):
        if self.has_flag("pretty"):
            self.write(json.dumps(obj, cls=PrettyJsonEncoder, indent=4, separators=(",", ": ")))
        else:
            self.write(json.dumps(obj, cls=NormalJsonEncoder))

        self.set_header("Content-Type", "application/json")
        self.set_status(status_code)
        self.finish()

    def _write_custom_error(self, exception):
        if isinstance(exception, BaseException):
            resp = exception.response
            self.write_json(resp, status_code=resp.get("status_code"))

    def write_error(self, status_code, **kwargs):
        if "exc_info" in kwargs:
            cls, exception, tb = kwargs.get("exc_info")
            if isinstance(exception, BaseException):
                self._write_custom_error(exception)
                self.write_logger(cls,exception,tb)
            elif status_code == 500:
                self.write_json(dict(error_code=500, error_message="internal server error"), status_code=500)
                self.write_logger(cls,exception,tb)
                self.write_to_slack(cls, exception, tb)
            elif status_code == 405:
                self.write_json(dict(error_code=405, error_message="method not supported"), status_code=405)
                self.write_logger(cls,exception,tb)
            else:
                super().write_error(status_code, **kwargs)
        else:
            super().write_error(status_code, **kwargs)

    def log_exception(self, cls, exception, tb):
        if not isinstance(exception, BaseException) or exception.log_exception:
            super().log_exception(cls, exception, tb)

    @tornado.gen.coroutine
    def write_to_slack(self, exception_cls, exception_instance, exception_tb):
        if self.application.slack is None:
            return
        yield tornado.gen.moment
        data = []
        format_string = "{0} : {1}"
        data.extend([
            format_string.format("Server Name", self.application.server_name)
        ])
        data.append("```")
        data.extend([t.strip() for t in traceback.format_tb(exception_tb)])
        data.append(" ".join([type(exception_instance).__name__, str(exception_instance)]))
        data.append("```")
        self.application.slack.send("\n".join(data), escape=False)

    ####################################################################
    def cget_json_argument(self, argument_name, default_value=None, is_required=False,
            argument_type=None, choices=None, multi=False):
        """Get argument from json body.

        argument_name           the name of the argument
        default_value           the default value for the argument, before choices
                                is applied. (default : False)
        is_required             if this argument is required, will raised ArgumentException if
                                required but not found or None. (default : False)
        argument_type           the type of the argument. (unlike cget_argument, json is not a
                                valid type)
        choices                 list or dictionary of the allowed values. If choices is dictionary,
                                the value will be converted using the key -> value mapping before
                                returning
        multi                   If true, the string will treated as a comma separated string.

        Note that choices will be converted only after type check
        """
        value = self.json_body.get(argument_name, default_value)

        if value is None:
            if is_required:
                raise ArgumentException(key=argument_name,
                    error_code=ArgumentException.MISSING_ARGUMENT,
                    error_message="{0} is required".format(argument_name))
            else:
                return value

        if not multi:
            if argument_type is not None:
                value, success = self._check_and_parse_type(argument_type=argument_type,
                        argument_value=value)

                if not success:
                    raise ArgumentException(key=argument_name,
                        error_code=ArgumentException.INVALID_ARGUMENT,
                        error_message="wrong type for {0} ".format(argument_name))

            if choices is not None:
                if value not in choices:
                    raise ArgumentException(key=argument_name,
                        error_code=ArgumentException.INVALID_ARGUMENT,
                        error_message="invalid value for {0} ".format(argument_name))
                if isinstance(choices, dict):
                    value = choices.get(value)
        else:
            if not isinstance(value, list):
                if not isinstance(value, str):
                    raise ArgumentException(key=argument_name,
                        error_code=ArgumentException.INVALID_ARGUMENT,
                        error_message="wrong type for {0} ".format(argument_name))
                values = value.split(",")

            value_success = [ _check_and_parse_type(v) for v in values ]
            if any([ v == False for _, v in value_success ]):
                raise ArgumentException(key=argument_name,
                    error_code=ArgumentException.INVALID_ARGUMENT,
                    error_message="wrong type for {0} ".format(argument_name))
            value = [ v for v, _ in value_success ]

        return value

    def cget_argument(self, argument_name, default_value=None, is_required=False,
            argument_type=None, choices=None, multi=False):
        """Get argument from json body.

        argument_name           the name of the argument
        default_value           the default value for the argument, before choices
                                is applied. (default : False)
        is_required             if this argument is required, will raised ArgumentException if
                                required but not found or None. (default : False)
        argument_type           the type of the argument (allowed type include default python type
                                like str, or json(the module))
        choices                 list or dictionary of the allowed values. If choices is dictionary,
                                the value will be converted using the key -> value mapping before
                                returning
        multi                   If true, the string will treated as a comma separated string.

        Note that choices will be converted only after type check
        """

        value = self.get_argument(argument_name, default_value)

        if value is None:
            if is_required:
                raise ArgumentException(key=argument_name,
                    error_code=ArgumentException.MISSING_ARGUMENT,
                    error_message="{0} is required".format(argument_name))
            else:
                return value

        if not multi:
            if argument_type is not None:
                value, success = self._check_and_parse_type(argument_type=argument_type,
                        argument_value=value)
                if not success:
                    raise ArgumentException(key=argument_name,
                        error_code=ArgumentException.INVALID_ARGUMENT,
                        error_message="wrong type for {0} ".format(argument_name))

            if choices is not None:
                if value not in choices:
                    raise ArgumentException(key=argument_name,
                        error_code=ArgumentException.INVALID_ARGUMENT,
                        error_message="invalid value for {0} ".format(argument_name))
                if isinstance(choices, dict):
                    value = choices.get(value)

        else:
            values = value.split(",")
            if argument_type is not None:
                value_success = [ self._check_and_parse_type(argument_type=argument_type, argument_value=v)
                        for v in values ]
                if any([ v == False for _, v in value_success ]):
                    raise ArgumentException(key=argument_name,
                        error_code=ArgumentException.INVALID_ARGUMENT,
                        error_message="wrong type for {0} ".format(argument_name))
                values = [ v for v, _ in value_success ]

            if choices is not None:
                if not all((v in choices for v in values)):
                    raise ArgumentException(key=argument_name,
                        error_code=ArgumentException.INVALID_ARGUMENT,
                        error_message="invalid value for {0} ".format(argument_name))
                if isinstance(choices, dict):
                    values = [ choices.get(v) for v in values ]
            value = values

        return value

    def _check_and_parse_type(self, *, argument_type, argument_value):
        """

        argument_type           argument_type, the type of the argument
                                (allowed typed are primitive types + "json")
        argument_value          the argument value to check

        return the value that is properly converted.

        Note for the behavior of each type

        str                     All values will be converted to str via str()
        int                     All values will be converted to int via int()
        float                   All values will be converted to float via float()

        json                    If already a list or dict, will return
                                If is a string, attempt to parse using json.loads()

        dict                    If a string, parse using json.loads and ensure that the result
                                is a dict.

        list                    If a string, parse using json.loads and ensure that the result
                                is a list.

        Other                   check using isinstance()

        if a tuple is parsed in, then it only check with the isinstance() method.

        return new_value, check_result
        """
        if isinstance(argument_type, tuple):
            if isinstance(argument_value, argument_type):
                return argument_value, True
            return None, False

        if argument_type in (str, int, float):
            try:
                new_value = argument_type(argument_value)
                return new_value, True
            except ValueError as e:
                return None, False

        if argument_type == json:
            if isinstance(argument_value, (list, dict)):
                return argument_value, True

            try:
                new_value = json.loads(value)
                return new_value, True
            except ValueError as e:
                return None, False

        if argument_type in (dict, list):
            if isinstance(argument_value, argument_type):
                return argument_value, True
            if isinstance(argument_value, str):
                try:
                    new_value = json.loads(value)
                    if isinstance(new_value, argument_type):
                        return None, False
                    return new_value, True
                except ValueError as e:
                    return None, False

        if isinstance(argument_value, argument_type):
            return argument_value, True

        return None, False

