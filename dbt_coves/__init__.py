import dbt.version

__version__ = "1.8.7"
__dbt_major_version__ = int(dbt.version.installed.major or 0)
__dbt_minor_version__ = int(dbt.version.installed.minor or 0)
__dbt_patch_version__ = int(dbt.version.installed.patch or 0)
