__version__ = "1.11.3"


def __getattr__(name: str):
    if name in ("__dbt_major_version__", "__dbt_minor_version__", "__dbt_patch_version__"):
        import dbt.version

        v = dbt.version.installed
        global __dbt_major_version__, __dbt_minor_version__, __dbt_patch_version__
        __dbt_major_version__ = int(v.major or 0)
        __dbt_minor_version__ = int(v.minor or 0)
        __dbt_patch_version__ = int(v.patch or 0)
        return globals()[name]
    raise AttributeError(f"module 'dbt_coves' has no attribute {name!r}")
