from dbt.task.base import ConfiguredTask


class BaseTask(ConfiguredTask):
    """
    Base task class
    """
    
    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        raise NotImplementedError()

    @classmethod
    def get_instance(cls, flags, coves_config):
        instance = super().from_args(flags.args)
        instance.coves_config = coves_config
        return instance

    def run(self) -> int:
        raise NotImplementedError()