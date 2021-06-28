from dbt.adapters.factory import get_adapter
from dbt.task.base import ConfiguredTask


class BaseTask:
    """
    Base Task Class
    """
    needs_config = False

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

    def __init__(self, args, config):
        super().__init__(args, config)
        self.adapter = get_adapter(self.config)
        self.coves_config = None
        self.coves_flags = None

    @classmethod
    def from_args(cls, args):
        config = cls.ConfigType.from_args(args)
        return cls(args, config)

    @classmethod
    def get_instance(cls, flags, coves_config):
        instance = cls.from_args(flags.args)
        instance.coves_config = coves_config
        instance.coves_flags = flags
        return instance
