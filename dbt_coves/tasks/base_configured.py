from dbt.task.base import ConfiguredTask

from dbt_coves.core.exceptions import MissingCommand
from dbt_coves.tasks.base import BaseTask
from dbt_coves.utils.mp_context import get_mp_context


class BaseConfiguredTask(ConfiguredTask, BaseTask):
    """
    Task class that requires a dbt configuration and adapter.
    """

    needs_config = True
    needs_dbt_project = True

    def __init__(self, args, config):
        from dbt.adapters.factory import get_adapter, register_adapter
        from dbt.context.providers import generate_runtime_macro_context
        from dbt.parser.manifest import ManifestLoader

        from dbt_coves import __dbt_major_version__, __dbt_minor_version__

        super().__init__(args, config)
        try:
            adapter = get_adapter(self.config)
        except KeyError:
            if (__dbt_major_version__, __dbt_minor_version__) < (1, 8):
                register_adapter(self.config)
            else:
                register_adapter(self.config, get_mp_context())
            adapter = get_adapter(self.config)

        if (__dbt_major_version__, __dbt_minor_version__) >= (1, 8):
            manifest = ManifestLoader.load_macros(
                self.config,
                adapter.connections.set_query_header,
                base_macros_only=True,
            )
            adapter.set_macro_resolver(manifest)
            adapter.set_macro_context_generator(generate_runtime_macro_context)

        self.adapter = adapter
        self.coves_config = None
        self.coves_flags = None

    @classmethod
    def from_args(cls, args):
        from dbt.config.runtime import RuntimeConfig

        from dbt_coves import __dbt_major_version__, __dbt_minor_version__

        try:
            from dbt.flags import set_flags

            set_flags(args)
        except ImportError:
            pass
        if (__dbt_major_version__, __dbt_minor_version__) < (1, 8):
            config = cls.ConfigType.from_args(args)
        else:
            from dbt_common.clients.system import get_env
            from dbt_common.context import set_invocation_context

            set_invocation_context(get_env())
            config = RuntimeConfig.from_args(args)
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
