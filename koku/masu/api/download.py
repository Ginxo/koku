#
# Copyright 2021 Red Hat Inc.
# SPDX-License-Identifier: Apache-2.0
#
"""View for temporary force download endpoint."""
import logging

from django.views.decorators.cache import never_cache
from rest_framework.decorators import api_view
from rest_framework.decorators import permission_classes
from rest_framework.decorators import renderer_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.settings import api_settings

from masu.celery.tasks import check_report_updates
from masu.verification.unattributed_storage import VerifyUnattributedStorage

LOG = logging.getLogger(__name__)


@never_cache
@api_view(http_method_names=["GET"])
@permission_classes((AllowAny,))
@renderer_classes(tuple(api_settings.DEFAULT_RENDERER_CLASSES))
def download_report(request):
    """Return download file async task ID."""
    params = request.query_params
    provider_uuid = params.get("provider_uuid")
    provider_type = params.get("provider_type")
    bill_date = params.get("bill_date")
    summarize_reports = params.get("summarize_reports", "true").lower()
    summarize_reports = True if summarize_reports == "true" else False
    async_download_result = check_report_updates.delay(
        provider_uuid=provider_uuid,
        provider_type=provider_type,
        bill_date=bill_date,
        summarize_reports=summarize_reports,
    )
    class_obj = VerifyUnattributedStorage("org1234567", "ca4f603c-1ece-4ef7-925f-e472a51494a3")
    class_obj.verify_cost()

    return Response({"Download Request Task ID": async_download_result})
    # class_obj = VerifyUnattributedStorage("org1234567", "ca4f603c-1ece-4ef7-925f-e472a51494a3")
    # class_obj.verify_cost()
    # return Response({"Download Request Task ID": "?"})
