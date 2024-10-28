/** @odoo-module */

import { ConfirmationDialog } from "@web/core/confirmation_dialog/confirmation_dialog";
import { CalendarController } from '@web/views/calendar/calendar_controller';
import { FormViewDialog } from '@web/views/view_dialogs/form_view_dialog';

import { serializeDate } from "@web/core/l10n/dates";

const { EventBus, useSubEnv } = owl;

export class DraftTimeOffCalendarController extends CalendarController {
    setup() {
        super.setup();
        useSubEnv({
            timeOffBus: new EventBus(),
        });
    }

    get employeeId() {
        return this.model.employeeId;
    }

    get filterPanelProps() {
        return {
            ...super.filterPanelProps,
            employee_id: this.employeeId,
        };
    }

    newTimeOffRequest() {
        const context = {};
        if (this.employeeId) {
            context['default_employee_id'] = this.employeeId;
        }
        if (this.model.meta.scale == 'day') {
            context['default_date_from'] = serializeDate(
                this.model.data.range.start.set({ hours: 7 }), "datetime"
            );
            context['default_date_to'] = serializeDate(
                this.model.data.range.end.set({ hours: 19 }), "datetime"
            );
        }

        this.displayDialog(FormViewDialog, {
            resModel: 'hr_holidays_draft.leave',
            title: this.env._t('New Draft'),
            viewId: this.model.formViewId,
            onRecordSaved: () => {
                this.model.load();
                this.env.timeOffBus.trigger('update_dashboard');
            },
            context: context,
        });
    }

    deleteRecord(record) {
        if (!record.can_cancel) {
            this.displayDialog(ConfirmationDialog, {
                title: this.env._t("Confirmation"),
                body: this.env._t("Are you sure you want to delete this record ?"),
                confirm: async () => {
                    await this.model.unlinkRecord(record.id);
                    this.env.timeOffBus.trigger('update_dashboard');
                },
                cancel: () => {},
            });
        } else {
            this.leaveCancelWizard(record.id, () => {
                this.model.load();
                this.env.timeOffBus.trigger('update_dashboard');
            });
        }
    }

}
DraftTimeOffCalendarController.template = "hr_holidays_draft.CalendarController";
DraftTimeOffCalendarController.components = {
    ...DraftTimeOffCalendarController.components,
}
