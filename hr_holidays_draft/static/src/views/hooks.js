/** @odoo-module */

import { useService } from "@web/core/utils/hooks";

const { useEnv } = owl;

export function useStressDays(props) {
    return (info) => {
        const date = luxon.DateTime.fromJSDate(info.date).toISODate();
        const stressDay = props.model.stressDays[date];
        if (stressDay) {
            const dayNumberElTop = info.view.el.querySelector(`.fc-day-top[data-date="${info.el.dataset.date }"]`)
            const dayNumberEl = info.view.el.querySelector(`.fc-day[data-date="${info.el.dataset.date }"]`)
            if (dayNumberElTop) {
                dayNumberElTop.classList.add(`hr_stress_day_top_${stressDay}`);
            }
            if (dayNumberEl) {
                dayNumberEl.classList.add(`hr_stress_day_${stressDay}`);
            }
        }
        return props.model.stressDays;
    }
}
