
import datetime

from collections import defaultdict
from datetime import time, timedelta

from odoo import api, fields, models
from odoo.tools import format_date
from odoo.addons.resource.models.resource import Intervals


class HolidaysType(models.Model):
    _inherit = "hr.leave.type"

    is_draft = fields.Boolean('Visible in Drafts', default=False)
    draft_virtual_leaves_taken = fields.Float(
        compute='_draft_compute_leaves', string='Virtual draft of the day off has already been taken')

    def _get_employees_days_per_allocation_draft(self, employee_ids, date=None):
        if not date:
            date = fields.Date.to_date(self.env.context.get('default_date_from')) or fields.Date.context_today(self)

        leaves_domain = [
            ('employee_id', 'in', employee_ids),
            ('holiday_status_id', 'in', self.ids)
        ]
        if self.env.context.get("ignore_future"):
            leaves_domain.append(('date_from', '<=', date))
        leaves = self.env['hr_holidays_draft.leave'].search(leaves_domain)

        allocations = self.env['hr.leave.allocation'].with_context(active_test=False).search([
            ('employee_id', 'in', employee_ids),
            ('holiday_status_id', 'in', self.ids),
        ])

        allocation_employees = defaultdict(lambda: defaultdict(list))

        for holiday_status_id in allocations.holiday_status_id:
            for employee_id in employee_ids:
                allocation_intervals = Intervals([(
                    fields.datetime.combine(allocation.date_from, time.min),
                    fields.datetime.combine(allocation.date_to or datetime.date.max, time.max),
                    allocation)
                    for allocation in allocations.filtered(lambda allocation: allocation.employee_id.id == employee_id and allocation.holiday_status_id == holiday_status_id)])

                allocation_employees[employee_id][holiday_status_id] = allocation_intervals

        leaves_employees = defaultdict(lambda: defaultdict(list))
        leave_intervals = []

        if leaves:
            for holiday_status_id in leaves.holiday_status_id:
                for employee_id in employee_ids:
                    leave_intervals = Intervals([(
                        fields.datetime.combine(leave.date_from, time.min),
                        fields.datetime.combine(leave.date_to, time.max),
                        leave)
                        for leave in leaves.filtered(lambda leave: leave.employee_id.id == employee_id and leave.holiday_status_id == holiday_status_id)])

                    leaves_employees[employee_id][holiday_status_id] = leave_intervals

        allocations_days_consumed = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: 0))))

        company_domain = [('company_id', 'in', list(set(self.env.company.ids + self.env.context.get('allowed_company_ids', []))))]

        if leaves_employees:
            for employee_id, leaves_interval_by_status in leaves_employees.items():
                for holiday_status_id in leaves_interval_by_status:
                    days_consumed = allocations_days_consumed[employee_id][holiday_status_id]
                    if allocation_employees[employee_id][holiday_status_id]:
                        allocations = allocation_employees[employee_id][holiday_status_id] & leaves_interval_by_status[holiday_status_id]
                        available_allocations = self.env['hr.leave.allocation']
                        for allocation_interval in allocations._items:
                            available_allocations |= allocation_interval[2]

                        sorted_available_allocations = available_allocations.filtered('date_to').sorted(key='date_to')
                        sorted_available_allocations += available_allocations.filtered(lambda allocation: not allocation.date_to)
                        leave_intervals = leaves_interval_by_status[holiday_status_id]._items
                        sorted_allocations_with_remaining_leaves = self.env['hr.leave.allocation']
                        for leave_interval in leave_intervals:
                            leaves = leave_interval[2]
                            for leave in leaves:
                                if leave.leave_type_request_unit in ['day', 'half_day']:
                                    leave_duration = leave.number_of_days
                                    leave_unit = 'days'
                                else:
                                    leave_duration = leave.number_of_hours_display
                                    leave_unit = 'hours'
                                if holiday_status_id.requires_allocation != 'no':
                                    for available_allocation in sorted_available_allocations:
                                        if (available_allocation.date_to and available_allocation.date_to < leave.date_from.date()) \
                                            or (available_allocation.date_from > leave.date_to.date()):
                                            continue
                                        virtual_remaining_leaves = (available_allocation.number_of_days if leave_unit == 'days' else available_allocation.number_of_hours_display) - allocations_days_consumed[employee_id][holiday_status_id][available_allocation]['draft_virtual_leaves_taken']
                                        max_leaves = min(virtual_remaining_leaves, leave_duration)
                                        days_consumed[available_allocation]['draft_virtual_leaves_taken'] += max_leaves
                                        if leave.state == 'validate':
                                            days_consumed[available_allocation]['leaves_taken'] += max_leaves
                                        leave_duration -= max_leaves

                                        if days_consumed[available_allocation]['virtual_remaining_leaves'] > 0 and available_allocation.date_to and available_allocation.date_to > date:
                                            sorted_allocations_with_remaining_leaves |= available_allocation
                                    if leave_duration > 0:
                                        days_consumed[False]['virtual_remaining_leaves'] -= leave_duration
                                else:
                                    days_consumed[False]['draft_virtual_leaves_taken'] += leave_duration
                                    if leave.state == 'validate':
                                        days_consumed[False]['leaves_taken'] += leave_duration
                        allocations_days_consumed[employee_id][holiday_status_id][False]['closest_allocation_to_expire'] = sorted_allocations_with_remaining_leaves[0] if sorted_allocations_with_remaining_leaves else False

        future_allocations_date_from = fields.datetime.combine(date, time.min)
        future_allocations_date_to = fields.datetime.combine(date, time.max) + timedelta(days=5*365)
        for employee_id, allocation_intervals_by_status in allocation_employees.items():
            employee = self.env['hr.employee'].browse(employee_id)
            for holiday_status_id, intervals in allocation_intervals_by_status.items():
                if not intervals:
                    continue
                future_allocation_intervals = intervals & Intervals([(
                    future_allocations_date_from,
                    future_allocations_date_to,
                    self.env['hr_holidays_draft.leave'])])
                search_date = date
                closest_allocations = self.env['hr.leave.allocation']
                for interval in intervals._items:
                    closest_allocations |= interval[2]
                allocations_with_remaining_leaves = self.env['hr.leave.allocation']
                for interval_from, interval_to, interval_allocations in future_allocation_intervals._items:
                    if interval_from.date() > search_date:
                        continue
                    interval_allocations = interval_allocations.filtered('active')
                    if not interval_allocations:
                        continue
                    employee_quantity_available = (
                        employee._get_work_days_data_batch(interval_from, interval_to, compute_leaves=False, domain=company_domain)[employee_id]
                        if interval_to != future_allocations_date_to
                        else {'days': float('inf'), 'hours': float('inf')}
                    )
                    for allocation in interval_allocations:
                        if allocation.date_from > search_date:
                            continue
                        days_consumed = allocations_days_consumed[employee_id][holiday_status_id][allocation]
                        if allocation.type_request_unit in ['day', 'half_day']:
                            quantity_available = employee_quantity_available['days']
                            remaining_days_allocation = (allocation.number_of_days - days_consumed['draft_virtual_leaves_taken'])
                        else:
                            quantity_available = employee_quantity_available['hours']
                            remaining_days_allocation = (allocation.number_of_hours_display - days_consumed['draft_virtual_leaves_taken'])
                        if quantity_available <= remaining_days_allocation:
                            search_date = interval_to.date() + timedelta(days=1)
                        days_consumed['virtual_remaining_leaves'] += min(quantity_available, remaining_days_allocation)
                        days_consumed['max_leaves'] = allocation.number_of_days if allocation.type_request_unit in ['day', 'half_day'] else allocation.number_of_hours_display
                        days_consumed['remaining_leaves'] = days_consumed['max_leaves'] - days_consumed['leaves_taken']
                        if remaining_days_allocation >= quantity_available:
                            break

                        if days_consumed['virtual_remaining_leaves'] > 0 and allocation.date_to and allocation.date_to > date:
                            allocations_with_remaining_leaves |= allocation
                allocations_sorted = sorted(allocations_with_remaining_leaves, key=lambda a: a.date_to)
                allocations_days_consumed[employee_id][holiday_status_id][False]['closest_allocation_to_expire'] = allocations_sorted[0] if allocations_sorted else False
        return allocations_days_consumed

    def get_employees_days_draft(self, employee_ids, date=None):
        result = {
            employee_id: {
                leave_type.id: {
                    'draft_virtual_leaves_taken': 0,
                } for leave_type in self
            } for employee_id in employee_ids
        }
        if not date:
            date = fields.Date.to_date(self.env.context.get('default_date_from')) or fields.Date.context_today(self)

        allocations_days_consumed = self._get_employees_days_per_allocation_draft(employee_ids, date)
        leave_keys = ['draft_virtual_leaves_taken']

        for employee_id in allocations_days_consumed:
            for holiday_status_id in allocations_days_consumed[employee_id]:
                for allocation in allocations_days_consumed[employee_id][holiday_status_id]:
                    if allocation:
                        if allocation.date_to and (allocation.date_to < date or allocation.date_from > date):
                            continue
                        for leave_key in leave_keys:
                            result[employee_id][holiday_status_id if isinstance(holiday_status_id, int) else holiday_status_id.id][leave_key] += allocations_days_consumed[employee_id][holiday_status_id][allocation][leave_key]
                    else:
                        result[employee_id][holiday_status_id if isinstance(holiday_status_id, int) else holiday_status_id.id]['closest_allocation_to_expire'] = allocations_days_consumed[employee_id][holiday_status_id][False]['closest_allocation_to_expire']
                        for leave_key in leave_keys:
                            if allocations_days_consumed[employee_id][holiday_status_id][False].get(leave_key):
                                result[employee_id][holiday_status_id if isinstance(holiday_status_id, int) else holiday_status_id.id][leave_key] = allocations_days_consumed[employee_id][holiday_status_id][False][leave_key]

        return result

    @api.depends_context('employee_id', 'default_employee_id')
    def _draft_compute_leaves(self):
        data_days = {}
        employee_id = self._get_contextual_employee_id()

        if employee_id:
            data_days = (self.get_employees_days_draft(employee_id)[employee_id[0]] if isinstance(employee_id, list) else
                         self.get_employees_days_draft([employee_id])[employee_id])

        for holiday_status in self:
            result = data_days.get(holiday_status.id, {})
            holiday_status.draft_virtual_leaves_taken = result.get('draft_virtual_leaves_taken', 0)

    def _get_days_request_draft(self):
        self.ensure_one()
        result = self._get_employees_days_per_allocation_draft(self.closest_allocation_to_expire.employee_id.ids)
        closest_allocation_remaining = 0
        if self.closest_allocation_to_expire:
            employee_allocations = result[self.closest_allocation_to_expire.employee_id.id][self].items()
            closest_allocation_remaining = sum(
                res['virtual_remaining_leaves']
                for alloc, res in employee_allocations
                if alloc and alloc.date_to == self.closest_allocation_to_expire.date_to
            )
        return (self.name, {
                'remaining_leaves': ('%.2f' % self.remaining_leaves).rstrip('0').rstrip('.'),
                'virtual_remaining_leaves': ('%.2f' % self.virtual_remaining_leaves).rstrip('0').rstrip('.'),
                'max_leaves': ('%.2f' % self.max_leaves).rstrip('0').rstrip('.'),
                'leaves_taken': ('%.2f' % self.leaves_taken).rstrip('0').rstrip('.'),
                'draft_virtual_leaves_taken': ('%.2f' % self.draft_virtual_leaves_taken).rstrip('0').rstrip('.'),
                'leaves_requested': ('%.2f' % (self.max_leaves - self.virtual_remaining_leaves - self.leaves_taken)).rstrip('0').rstrip('.'),
                'leaves_approved': ('%.2f' % self.leaves_taken).rstrip('0').rstrip('.'),
                'closest_allocation_remaining': ('%.2f' % closest_allocation_remaining).rstrip('0').rstrip('.'),
                'closest_allocation_expire': format_date(self.env, self.closest_allocation_to_expire.date_to) if self.closest_allocation_to_expire.date_to else False,
                'request_unit': self.request_unit,
                'icon': self.sudo().icon_id.url,
                }, self.requires_allocation, self.id)

    @api.model
    def get_days_all_request_hr_holidays_draft(self):
        leave_types = sorted(self.search([('is_draft', '=', True)]).filtered(lambda x: x.draft_virtual_leaves_taken > 0), key=self._model_sorting_key, reverse=True)

        return [lt._get_days_request_draft() for lt in leave_types]
