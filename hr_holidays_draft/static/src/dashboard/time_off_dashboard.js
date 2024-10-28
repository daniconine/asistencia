/* @odoo-module */
import { DraftTimeOffCard } from './time_off_card';
import { useBus, useService } from "@web/core/utils/hooks";

const { Component, useState, onWillStart } = owl;

export class DraftTimeOffDashboard extends Component {
    setup() {
        this.orm = useService("orm");
        this.state = useState({
            holidays: [],
        });
        useBus(this.env.timeOffBus, 'update_dashboard', async () => {
            await this.loadDashboardData()
        });

        onWillStart(async () => {
            await this.loadDashboardData();
        });

    }
    
    async loadDashboardData() {
        const context = {};
        if (this.props.employeeId !== null) {
            context['employee_id'] = this.props.employeeId;
        }

        this.state.holidays = await this.orm.call(
            'hr.leave.type',
            'get_days_all_request_hr_holidays_draft',
            [],
            {
                context: context
            }
        );
    }
}

DraftTimeOffDashboard.components = { DraftTimeOffCard };
DraftTimeOffDashboard.template = 'hr_holidays_draft.TimeOffDashboard';
DraftTimeOffDashboard.props = ['employeeId'];
