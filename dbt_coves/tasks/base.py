from dbt.adapters.factory import get_adapter
from dbt.task.base import ConfiguredTask


class BaseTask(ConfiguredTask):
    """
    Base task class
    """

    def __init__(self, args, config):
        super().__init__(args, config)
        self.adapter = get_adapter(self.config)

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
