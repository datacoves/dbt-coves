from dbt_coves.tasks.base import NonDbtBaseTask


class BaseSetupException(Exception):
    pass


class BaseSetupTask(NonDbtBaseTask):
    """
    Provides common functionality for all "Setup" sub tasks.
    """

    arg_parser = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self) -> int:
        raise NotImplementedError()

    def get_config_value(self, key):
        return self.coves_config.integrated["setup"][self.args.task][key]
