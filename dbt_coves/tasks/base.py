from dbt.adapters.factory import get_adapter
from dbt.task.base import ConfiguredTask

from dbt_coves.core.exceptions import MissingCommand


class BaseTask:
    """
    Base Task Class
    """

    needs_config = False
    needs_dbt_project = False

    def __init__(self, args, config=None):
        self.args = args

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        raise NotImplementedError()

    @classmethod
    def get_instance(cls, flags, coves_config=None):
        instance = cls(flags.args)
        instance.coves_flags = flags
        return instance

    def run(self) -> int:
        raise NotImplementedError()


class BaseConfiguredTask(ConfiguredTask, BaseTask):
    """
    Task class that requires a configuration
    """

    needs_config = True
    needs_dbt_project = True

    def __init__(self, args, config):
        super().__init__(args, config)
        self.adapter = get_adapter(self.config)
        self.coves_config = None
        self.coves_flags = None

    @classmethod
    def from_args(cls, args):
        config = cls.ConfigType.from_args(args)
        try:
            return cls(args, config)
        # class cannot be instantiated with abstract methods.
        # that means a subcommand is missing.
        except TypeError:
            raise MissingCommand(cls.arg_parser)

    @classmethod
    def get_instance(cls, flags, coves_config):
        instance = cls.from_args(flags.args)
        instance.coves_config = coves_config
        instance.coves_flags = flags
        return instance


class NonDbtBaseTask(BaseTask):
    """
    Task class that requires a configuration
    """

    needs_config = True
    needs_dbt_project = False

    def __init__(self, args, config):
        super().__init__(args, config)
        self.coves_config = config
        self.coves_flags = None

    @classmethod
    def from_args(cls, args):
        config = cls.ConfigType.from_args(args)
        return cls(args, config)

    @classmethod
    def get_instance(cls, flags, coves_config):
        instance = cls(flags.args, coves_config)
        instance.coves_config = coves_config
        instance.coves_flags = flags
        return instance

    @classmethod
    def run(cls) -> int:
        raise MissingCommand(cls.arg_parser)


class NonDbtBaseConfiguredTask(BaseTask):
    """
    Task class that requires a configuration
    """

    needs_config = True
    needs_dbt_project = False

    def __init__(self, args, config):
        super().__init__(args, config)
        self.coves_config = config
        self.coves_flags = None

    @classmethod
    def from_args(cls, args):
        config = cls.ConfigType.from_args(args)
        return cls(args, config)

    @classmethod
    def get_instance(cls, flags, coves_config):
        instance = cls(flags.args, coves_config)
        instance.coves_config = coves_config
        instance.coves_flags = flags
        return instance
