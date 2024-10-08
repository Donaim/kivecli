
class UserError(RuntimeError):
    def __init__(self, fmt: str, *fmt_args: object):
        self.fmt = fmt
        self.fmt_args = fmt_args
        self.code = 1

    def __str__(self) -> str:
        return self.fmt % self.fmt_args
