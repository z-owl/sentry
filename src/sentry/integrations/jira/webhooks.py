from __future__ import absolute_import
import logging

from django.views.decorators.csrf import csrf_exempt

from sentry.api.base import Endpoint

from sentry.integrations.atlassian_connect import AtlassianConnectValidationError, get_integration_from_jwt
from sentry.models import (
    sync_group_assignee_inbound, Activity, Group, GroupStatus, ProjectIntegration
)

logger = logging.getLogger('sentry.integrations.jira.webhooks')


class JiraIssueUpdatedWebhook(Endpoint):
    authentication_classes = ()
    permission_classes = ()

    @csrf_exempt
    def dispatch(self, request, *args, **kwargs):
        return super(JiraIssueUpdatedWebhook, self).dispatch(request, *args, **kwargs)

    def handle_assignee_change(self, integration, data):
        assignee = data['issue']['fields']['assignee']
        issue_key = data['issue']['key']

        if assignee is None:
            sync_group_assignee_inbound(
                integration, None, issue_key, assign=False,
            )
        else:
            sync_group_assignee_inbound(
                integration, assignee['emailAddress'], issue_key, assign=True,
            )

    def handle_status_change(self, integration, data):
        issue_key = data['issue']['key']

        try:
            change_log = next(
                item for item in data['changelog']['items'] if item['field'] == 'status'
            )
        except StopIteration:
            logger.info(
                'missing-changelog', extra={
                    'issue_key': issue_key,
                    'integration_id': integration.id,
                }
            )
            return

        affected_groups = list(
            Group.objects.get_groups_by_external_issue(
                integration, issue_key,
            ).select_related('project'),
        )

        project_integration_configs = {
            pi.project_id: pi.config for pi in ProjectIntegration.objects.filter(
                project_id__in=[g.project_id for g in affected_groups]
            )
        }

        groups_to_resolve = []
        groups_to_unresolve = []

        for group in affected_groups:
            project_config = project_integration_configs.get(group.project_id, {})
            resolve_when = project_config.get('resolve_when')
            unresolve_when = project_config.get('unresolve_when')
            # TODO(jess): make sure config validation doesn't
            # allow these to be the same
            if (unresolve_when and resolve_when) and (resolve_when == unresolve_when):
                logger.warning(
                    'project-config-conflict', extra={
                        'project_id': group.project_id,
                        'integration_id': integration.id,
                    }
                )
                continue

            if change_log['to'] == unresolve_when:
                groups_to_unresolve.append(group)
            elif change_log['to'] == resolve_when:
                groups_to_resolve.append(group)

        if groups_to_resolve:
            updated_resolve = Group.objects.filter(
                id__in=[g.id for g in groups_to_resolve],
            ).exclude(
                status=GroupStatus.RESOLVED,
            ).update(
                status=GroupStatus.RESOLVED,
            )
            if updated_resolve:
                for group in groups_to_resolve:
                    Activity.objects.create(
                        project=group.project,
                        group=group,
                        type=Activity.SET_RESOLVED,
                    )

        if groups_to_unresolve:
            updated_unresolve = Group.objects.filter(
                id__in=[g.id for g in groups_to_resolve],
            ).exclude(
                status=GroupStatus.UNRESOLVED,
            ).update(
                status=GroupStatus.UNRESOLVED,
            )
            if updated_unresolve:
                for group in groups_to_unresolve:
                    Activity.objects.create(
                        project=group.project,
                        group=group,
                        type=Activity.SET_UNRESOLVED,
                    )

    def post(self, request, *args, **kwargs):
        try:
            token = request.META['HTTP_AUTHORIZATION'].split(' ', 1)[1]
        except (KeyError, IndexError):
            return self.respond(status=400)

        data = request.DATA

        assignee_changed = any(
            item for item in data['changelog']['items'] if item['field'] == 'assignee'
        )

        status_changed = any(
            item for item in data['changelog']['items'] if item['field'] == 'status'
        )

        if assignee_changed or status_changed:
            try:
                integration = get_integration_from_jwt(
                    token, request.path, request.GET, method='POST'
                )
            except AtlassianConnectValidationError:
                return self.respond(status=400)

            if assignee_changed:
                self.handle_assignee_change(integration, data)

            if status_changed:
                self.handle_status_change(integration, data)

        return self.respond()
