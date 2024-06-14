import logging
import os

from django.db.models import Sum
from django_tenants.utils import schema_context
from trino.exceptions import TrinoExternalError
from trino.exceptions import TrinoUserError

import koku.trino_database as trino_db
from masu.celery.common import COST_VERIFICATION_TASK
from masu.celery.common import UPDATE_SUMMARY_TABLES_QUEUE
from reporting.models import OCPUsageLineItemDailySummary
from reporting_common.models import DelayedCeleryTasks


LOG = logging.getLogger(__name__)


def get_module_path():
    current_file_path = os.path.abspath(__file__)
    package_root = os.path.dirname(current_file_path)

    # Traverse up to find the package root (the directory containing __init__.py)
    while package_root and "__init__.py" in os.listdir(package_root):
        package_root = os.path.dirname(package_root)

    # Create the module path by replacing os separators with dots
    relative_path = os.path.relpath(current_file_path, start=package_root)
    module_path = os.path.splitext(relative_path)[0].replace(os.sep, ".")

    return module_path


def run_trino_sql(sql, schema=None):
    retries = 5
    for i in range(retries):
        try:
            with trino_db.connect(schema=schema) as conn:
                cur = conn.cursor()
                cur.execute(sql)
                return cur.fetchall()
        except TrinoExternalError as err:
            if err.error_name == "HIVE_METASTORE_ERROR" and i < (retries - 1):
                continue
            else:
                raise err
        except TrinoUserError as err:
            LOG.info(err.message)
            return


class VerifyUnattributedStorage:
    """
    Verify the unattributed storage cost.
    """

    def __init__(self, schema_name, provider_uuid):
        self.schema_name = schema_name
        # This is an OCP cost, use the OCP provider_uuid
        self.provider_uuid = provider_uuid

    def create_delayed_task(self):
        """Create a delayed celery task for verify cost."""
        task_kwargs = {
            "class_name": self.__class__.__name__,
            "module_path": get_module_path(),
        }
        DelayedCeleryTasks.create_or_reset_timeout(
            task_name=COST_VERIFICATION_TASK,
            task_args=[self.schema_name, self.provider_uuid],
            task_kwargs=task_kwargs,
            provider_uuid=self.provider_uuid,
            queue_name=UPDATE_SUMMARY_TABLES_QUEUE,
        )

    def verify_cost(self, **kwargs):
        """
        Verify the unattributed storage cost.
        """
        with schema_context(self.schema_name):

            resource_ids = OCPUsageLineItemDailySummary.objects.filter(namespace="Storage unattributed").values_list(
                "resource_id", flat=True
            )
            total_sums = (
                OCPUsageLineItemDailySummary.objects.filter(resource_id__in=resource_ids)
                .values("resource_id")
                .annotate(total_infrastructure_raw_cost=Sum("infrastructure_raw_cost"))
                .order_by("resource_id")
            )
            LOG.info("\n")
            LOG.info("POSTGRESQL RESOURCE TOTALS")
            for resource in total_sums:
                LOG.info(resource)

            LOG.info("TRINO RESOURCE TOTALS")
            trino_query = """
            select
                resource_id,
                sum(pretax_cost) + sum(markup_cost) as total_cost
            from reporting_ocpazurecostlineitem_project_daily_summary
            where resource_id in ('pv-123-claimless', 'azure-cloud-prefix-pvc-partial-matching', 'disk-id-1234567')
            group by resource_id order by resource_id
            """
            results = run_trino_sql(trino_query, "org1234567")
            for result in results:
                LOG.info(result)

            # Other Checks
            # 1) Ensure that the ocp on azure cost for resources are not
            # higher than the azure only cost for resources
