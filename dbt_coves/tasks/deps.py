import dbt.utils
import dbt.deprecations
import dbt.exceptions

from dbt.config import RuntimeConfig

from dbt.logger import GLOBAL_LOGGER as logger

from dbt.task.base import BaseTask, move_to_nearest_project_dir


class DepsTask(BaseTask):
    ConfigType = RuntimeConfig

    def __init__(self, args, config: RuntimeConfig):
        super().__init__(args=args, config=config)

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "deps", parents=[base_subparser], help="Test deps."
        )
        subparser.set_defaults(cls=cls)
        return subparser

    def track_package_install(
        self, package_name: str, source_type: str, version: str
    ) -> None:
        # Hub packages do not need to be hashed, as they are public
        # Use the string 'local' for local package versions
        if source_type == 'local':
            package_name = dbt.utils.md5(package_name)
            version = 'local'
        elif source_type != 'hub':
            package_name = dbt.utils.md5(package_name)
            version = dbt.utils.md5(version)

        dbt.tracking.track_package_install(
            self.config,
            self.config.args,
            {
                "name": package_name,
                "source": source_type,
                "version": version
            }
        )

    def run(self):
        logger.info('Test message')

    @classmethod
    def from_args(cls, parser):
        # deps needs to move to the project directory, as it does put files
        # into the modules directory
        move_to_nearest_project_dir(parser.args)
        return super().from_args(parser.args)