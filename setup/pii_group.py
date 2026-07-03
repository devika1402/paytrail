"""Provision the ``paytrail_pii_readers`` group and grant the ETL principal.

Why this is required, not cosmetic: silver reads the real ``nameOrig``/``nameDest``
to conform and tokenise account keys. Those columns carry the Unity Catalog mask
``paytrail.bronze.mask_account`` (docs/GOVERNANCE.md), which redacts to ``***``+last4
for anyone outside this group. If the pipeline principal is NOT a member, silver
reads masked values whose ``*`` prefix fails the actor contract and **every row is
quarantined**. Membership gives the ETL identity its lawful basis to process; every
consumer principal stays masked.

Idempotent: creates the workspace group only if absent, adds the caller only if not
already a member. Auth is the ambient Databricks CLI profile (no secret here).

Usage:
    python setup/pii_group.py
"""

from __future__ import annotations

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam

GROUP_NAME = "paytrail_pii_readers"


def ensure_group(client: WorkspaceClient) -> str:
    """Return the id of the pii-readers group, creating it if it does not exist."""
    for group in client.groups.list(filter=f'displayName eq "{GROUP_NAME}"'):
        if group.id is not None:
            print(f"[pii_group] group exists: {GROUP_NAME} ({group.id})")
            return group.id
    created = client.groups.create(display_name=GROUP_NAME)
    if created.id is None:
        raise RuntimeError(f"Group creation returned no id for {GROUP_NAME}")
    print(f"[pii_group] created group: {GROUP_NAME} ({created.id})")
    return created.id


def ensure_member(client: WorkspaceClient, group_id: str, user_id: str) -> None:
    """Add ``user_id`` to the group unless already a member (idempotent patch)."""
    group = client.groups.get(group_id)
    members = group.members or []
    if any(member.value == user_id for member in members):
        print(f"[pii_group] principal {user_id} already a member, no-op")
        return
    client.groups.patch(
        group_id,
        operations=[
            iam.Patch(
                op=iam.PatchOp.ADD,
                path="members",
                value=[{"value": user_id}],
            )
        ],
        schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
    )
    print(f"[pii_group] added principal {user_id} to {GROUP_NAME}")


def main() -> int:
    """Create the group and grant the current principal, idempotently."""
    client = WorkspaceClient()
    me = client.current_user.me()
    if me.id is None:
        raise RuntimeError("Could not resolve the current principal's id.")
    group_id = ensure_group(client)
    ensure_member(client, group_id, me.id)
    print(f"[pii_group] done, {me.user_name} can read raw identifiers for ETL.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
