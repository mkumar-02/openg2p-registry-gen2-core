from typing import Any, List, Optional

from openg2p_fastapi_common.schemas import G2PRequest, G2PRequestBody, G2PResponse, G2PResponseBody
from pydantic import BaseModel, Field


class ListMyAweTasksRequestPayload(BaseModel):
    """List tasks for the current user (assignee=me).

    Omit ``status`` to return tasks in every status (open, claimed, completed, …).
    Pass ``status='open'`` to return only open tasks.
    """

    request_id: Optional[str] = None
    status: Optional[str] = Field(
        default=None,
        description="Filter by task status (open, claimed, completed, …). Omit for all.",
    )
    artifact_type: Optional[str] = None
    policy_key: Optional[str] = None
    search_text: Optional[str] = None
    page: int = 1
    page_size: int = 25


class MyAweTaskStatsRequestPayload(BaseModel):
    status: Optional[str] = Field(
        default=None,
        description="Filter by task status. Omit to count tasks in every status.",
    )


class SubmitAweTaskDecisionRequestPayload(BaseModel):
    task_id: str
    action: str = Field(description="approve, reject, or abstain")
    comment: Optional[str] = None
    attachments_ref: Optional[str] = None
    artifact_id: str = Field(
        description="Registry artifact id (change_request_id or submission_id)",
    )
    artifact_type: str = Field(
        description="AWE artifact type, e.g. registry.change_request",
    )
    current_stage: int = Field(
        ge=1,
        description="Stage order the client saw when loading the approval UI",
    )


class ClaimAweTaskRequestPayload(BaseModel):
    task_id: str


class GetAweRequestRequestPayload(BaseModel):
    request_id: str


class GetAweRequestEventsRequestPayload(BaseModel):
    request_id: str


class AweProxyDataResponsePayload(BaseModel):
    data: Any


class ListMyAweTasksRequestBody(G2PRequestBody):
    request_payload: ListMyAweTasksRequestPayload


class ListMyAweTasksRequest(G2PRequest):
    request_body: ListMyAweTasksRequestBody


class MyAweTaskStatsRequestBody(G2PRequestBody):
    request_payload: MyAweTaskStatsRequestPayload


class MyAweTaskStatsRequest(G2PRequest):
    request_body: MyAweTaskStatsRequestBody


class SubmitAweTaskDecisionRequestBody(G2PRequestBody):
    request_payload: SubmitAweTaskDecisionRequestPayload


class SubmitAweTaskDecisionRequest(G2PRequest):
    request_body: SubmitAweTaskDecisionRequestBody


class ClaimAweTaskRequestBody(G2PRequestBody):
    request_payload: ClaimAweTaskRequestPayload


class ClaimAweTaskRequest(G2PRequest):
    request_body: ClaimAweTaskRequestBody


class GetAweRequestRequestBody(G2PRequestBody):
    request_payload: GetAweRequestRequestPayload


class GetAweRequestRequest(G2PRequest):
    request_body: GetAweRequestRequestBody


class GetAweRequestEventsRequestBody(G2PRequestBody):
    request_payload: GetAweRequestEventsRequestPayload


class GetAweRequestEventsRequest(G2PRequest):
    request_body: GetAweRequestEventsRequestBody


class AweProxyDataResponseBody(G2PResponseBody):
    response_payload: Optional[AweProxyDataResponsePayload] = None


class AweProxyDataResponse(G2PResponse):
    response_body: Optional[AweProxyDataResponseBody] = None


class AweProxyListDataResponseBody(G2PResponseBody):
    response_payload: Optional[List[Any]] = None


class AweProxyListDataResponse(G2PResponse):
    response_body: Optional[AweProxyListDataResponseBody] = None
