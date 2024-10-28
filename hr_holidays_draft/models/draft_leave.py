# -*- coding: utf-8 -*-

from datetime import datetime, time
from pytz import timezone, UTC

from odoo import api, fields, models, tools
from odoo.addons.base.models.res_partner import _tz_get
from odoo.addons.resource.models.resource import float_to_time, HOURS_PER_DAY
from odoo.exceptions import AccessError, UserError, ValidationError
from odoo.tools.float_utils import float_round
from odoo.tools.translate import _
from odoo.osv import expression
from collections import namedtuple


DummyAttendance = namedtuple('DummyAttendance', 'hour_from, hour_to, dayofweek, day_period, week_type')


class HolidaysRequest(models.Model):
    _name = "hr_holidays_draft.leave"
    _description = "Draft of Time Off"
    _inherit = ['mail.thread', 'mail.activity.mixin']

    hr_leave_id = fields.Many2one('hr.leave', string='Leave')

    active = fields.Boolean(default=True, readonly=True)
    # description
    name = fields.Char('Description', compute='_compute_description', inverse='_inverse_description',
                       search='_search_description', compute_sudo=False)

    private_name = fields.Char('Time Off Description', groups='hr_holidays.group_hr_holidays_user')

    holiday_type = fields.Selection([
        ('employee', 'By Employee'),
        ('company', 'By Company'),
        ('department', 'By Department'),
        ('category', 'By Employee Tag')],
        string='Allocation Mode', readonly=True, required=True, default='employee',
        states={'draft': [('readonly', False)], 'confirm': [('readonly', False)]},
        help='By Employee: Allocation/Request for individual Employee, By Employee Tag: Allocation/Request for group of employees in category')

    holiday_status_id = fields.Many2one(
        "hr.leave.type", compute='_compute_from_employee_id', store=True, string="Time Off Type", required=True,
        readonly=False,
        states={'cancel': [('readonly', True)], 'refuse': [('readonly', True)], 'validate1': [('readonly', True)],
                'validate': [('readonly', True)]},
        domain="["
               "('company_id', '?=', employee_company_id), "
               "'|', "
               "('requires_allocation', '=', 'no'), "
               "('has_valid_allocation', '=', True)"
               "]",
        tracking=True)

    state = fields.Selection([
        ('draft', 'To Submit'),
        ('confirm', 'To Approve'),
        ('refuse', 'Refused'),
        ('validate1', 'Second Approval'),
        ('validate', 'Approved')
    ], string='Status', compute='_compute_state', store=True, tracking=True, copy=False, readonly=False)

    report_note = fields.Text('HR Comments', copy=False, groups="hr_holidays.group_hr_holidays_manager")

    user_id = fields.Many2one('res.users', string='User', related='employee_id.user_id', related_sudo=True,
                              compute_sudo=True, store=True, readonly=True, index=True)

    employee_id = fields.Many2one(
        'hr.employee', default=lambda self: self.env.user.employee_id, string='Employee', index=True, readonly=True,
        ondelete="restrict")
    manager_id = fields.Many2one('hr.employee', compute='_compute_from_employee_id', store=True, readonly=False)

    holiday_allocation_id = fields.Many2one(
        'hr.leave.allocation', compute='_compute_from_holiday_status_id', string="Allocation", store=True,
        readonly=False)

    color = fields.Integer("Color", related='holiday_status_id.color')

    validation_type = fields.Selection(string='Validation Type', related='holiday_status_id.leave_validation_type',
                                       readonly=False)

    date_from = fields.Datetime(
        'Start Date', compute='_compute_date_from_to', store=True, readonly=False, index=True, copy=False,
        required=True, tracking=True,
        states={'cancel': [('readonly', True)], 'refuse': [('readonly', True)], 'validate1': [('readonly', True)],
                'validate': [('readonly', True)]})

    date_to = fields.Datetime(
        'End Date', compute='_compute_date_from_to', store=True, readonly=False, copy=False, required=True,
        tracking=True,
        states={'cancel': [('readonly', True)], 'refuse': [('readonly', True)], 'validate1': [('readonly', True)],
                'validate': [('readonly', True)]})

    request_date_from_period = fields.Selection([
        ('am', 'Morning'), ('pm', 'Afternoon')],
        string="Date Period Start", default='am')

    request_hour_from = fields.Selection([
        ('0', '12:00 AM'), ('0.5', '12:30 AM'),
        ('1', '1:00 AM'), ('1.5', '1:30 AM'),
        ('2', '2:00 AM'), ('2.5', '2:30 AM'),
        ('3', '3:00 AM'), ('3.5', '3:30 AM'),
        ('4', '4:00 AM'), ('4.5', '4:30 AM'),
        ('5', '5:00 AM'), ('5.5', '5:30 AM'),
        ('6', '6:00 AM'), ('6.5', '6:30 AM'),
        ('7', '7:00 AM'), ('7.5', '7:30 AM'),
        ('8', '8:00 AM'), ('8.5', '8:30 AM'),
        ('9', '9:00 AM'), ('9.5', '9:30 AM'),
        ('10', '10:00 AM'), ('10.5', '10:30 AM'),
        ('11', '11:00 AM'), ('11.5', '11:30 AM'),
        ('12', '12:00 PM'), ('12.5', '12:30 PM'),
        ('13', '1:00 PM'), ('13.5', '1:30 PM'),
        ('14', '2:00 PM'), ('14.5', '2:30 PM'),
        ('15', '3:00 PM'), ('15.5', '3:30 PM'),
        ('16', '4:00 PM'), ('16.5', '4:30 PM'),
        ('17', '5:00 PM'), ('17.5', '5:30 PM'),
        ('18', '6:00 PM'), ('18.5', '6:30 PM'),
        ('19', '7:00 PM'), ('19.5', '7:30 PM'),
        ('20', '8:00 PM'), ('20.5', '8:30 PM'),
        ('21', '9:00 PM'), ('21.5', '9:30 PM'),
        ('22', '10:00 PM'), ('22.5', '10:30 PM'),
        ('23', '11:00 PM'), ('23.5', '11:30 PM')], string='Hour from')

    request_hour_to = fields.Selection([
        ('0', '12:00 AM'), ('0.5', '12:30 AM'),
        ('1', '1:00 AM'), ('1.5', '1:30 AM'),
        ('2', '2:00 AM'), ('2.5', '2:30 AM'),
        ('3', '3:00 AM'), ('3.5', '3:30 AM'),
        ('4', '4:00 AM'), ('4.5', '4:30 AM'),
        ('5', '5:00 AM'), ('5.5', '5:30 AM'),
        ('6', '6:00 AM'), ('6.5', '6:30 AM'),
        ('7', '7:00 AM'), ('7.5', '7:30 AM'),
        ('8', '8:00 AM'), ('8.5', '8:30 AM'),
        ('9', '9:00 AM'), ('9.5', '9:30 AM'),
        ('10', '10:00 AM'), ('10.5', '10:30 AM'),
        ('11', '11:00 AM'), ('11.5', '11:30 AM'),
        ('12', '12:00 PM'), ('12.5', '12:30 PM'),
        ('13', '1:00 PM'), ('13.5', '1:30 PM'),
        ('14', '2:00 PM'), ('14.5', '2:30 PM'),
        ('15', '3:00 PM'), ('15.5', '3:30 PM'),
        ('16', '4:00 PM'), ('16.5', '4:30 PM'),
        ('17', '5:00 PM'), ('17.5', '5:30 PM'),
        ('18', '6:00 PM'), ('18.5', '6:30 PM'),
        ('19', '7:00 PM'), ('19.5', '7:30 PM'),
        ('20', '8:00 PM'), ('20.5', '8:30 PM'),
        ('21', '9:00 PM'), ('21.5', '9:30 PM'),
        ('22', '10:00 PM'), ('22.5', '10:30 PM'),
        ('23', '11:00 PM'), ('23.5', '11:30 PM')], string='Hour to')

    request_date_from = fields.Date('Request Start Date')

    request_date_to = fields.Date('Request End Date')

    request_unit_half = fields.Boolean('Half Day', compute='_compute_request_unit_half', store=True, readonly=False)

    request_unit_hours = fields.Boolean('Custom Hours', compute='_compute_request_unit_hours', store=True,
                                        readonly=False)

    employee_company_id = fields.Many2one(related='employee_id.company_id', readonly=True, store=True)

    active_employee = fields.Boolean(related='employee_id.active', string='Employee Active', readonly=True)

    department_id = fields.Many2one(
        'hr.department', compute='_compute_department_id', store=True, string='Department', readonly=False,
        states={'cancel': [('readonly', True)], 'refuse': [('readonly', True)], 'validate1': [('readonly', True)],
                'validate': [('readonly', True)]})

    mode_company_id = fields.Many2one(
        'res.company', compute='_compute_from_holiday_type', store=True, string='Company Mode',
        states={'draft': [('readonly', False)], 'confirm': [('readonly', False)]})

    tz_mismatch = fields.Boolean(compute='_compute_tz_mismatch')

    tz = fields.Selection(_tz_get, compute='_compute_tz')

    notes = fields.Text('Reasons', readonly=True,
                        states={'draft': [('readonly', False)], 'confirm': [('readonly', False)]})

    number_of_days = fields.Float(
        'Duration (Days)', compute='_compute_number_of_days', store=True, readonly=False, copy=False, tracking=True)

    number_of_days_display = fields.Float(
        'Duration in days', compute='_compute_number_of_days_display', readonly=True)

    number_of_hours_display = fields.Float(
        'Duration in hours', compute='_compute_number_of_hours_display', readonly=True)

    number_of_hours_text = fields.Char(compute='_compute_number_of_hours_text')

    duration_display = fields.Char(
        'Requested (Days/Hours)', compute='_compute_duration_display', store=True)

    meeting_id = fields.Many2one('calendar.event', string='Meeting', copy=False)

    parent_id = fields.Many2one('hr_holidays_draft.leave', string='Parent', copy=False)

    linked_request_ids = fields.One2many('hr_holidays_draft.leave', 'parent_id', string='Linked Requests')

    category_id = fields.Many2one(
        'hr.employee.category', compute='_compute_from_holiday_type', store=True, string='Employee Tag',
        states={'draft': [('readonly', False)], 'confirm': [('readonly', False)]}, help='Category of Employee')

    first_approver_id = fields.Many2one(
        'hr.employee', string='First Approval', readonly=True, copy=False)

    second_approver_id = fields.Many2one(
        'hr.employee', string='Second Approval', readonly=True, copy=False)

    can_reset = fields.Boolean('Can reset', compute='_compute_can_reset')

    can_approve = fields.Boolean('Can Approve', compute='_compute_can_approve')

    can_cancel = fields.Boolean('Can Cancel', compute='_compute_can_cancel')

    attachment_ids = fields.One2many('ir.attachment', 'res_id', string="Attachments")

    leave_type_support_document = fields.Boolean(related="holiday_status_id.support_document")

    supported_attachment_ids = fields.Many2many(
        'ir.attachment', string="Attach File", compute='_compute_supported_attachment_ids',
        inverse='_inverse_supported_attachment_ids')

    supported_attachment_ids_count = fields.Integer(compute='_compute_supported_attachment_ids')

    leave_type_request_unit = fields.Selection(related='holiday_status_id.request_unit', readonly=True)

    is_hatched = fields.Boolean('Hatched', compute='_compute_is_hatched')

    is_striked = fields.Boolean('Striked', compute='_compute_is_hatched')

    has_stress_day = fields.Boolean(compute='_compute_has_stress_day')

    def _inverse_supported_attachment_ids(self):
        for holiday in self:
            holiday.attachment_ids = holiday.supported_attachment_ids

    def _inverse_description(self):
        is_officer = self.user_has_groups('hr_holidays.group_hr_holidays_user')

        for leave in self:
            if is_officer or leave.user_id == self.env.user or leave.employee_id.leave_manager_id == self.env.user:
                leave.sudo().private_name = leave.name

    @api.depends_context('uid')
    @api.depends('state', 'employee_id')
    def _compute_can_cancel(self):
        now = fields.Datetime.now()
        for leave in self:
            leave.can_cancel = leave.id and leave.employee_id.user_id == self.env.user and leave.state == 'validate' and leave.date_from and leave.date_from > now

    @api.depends_context('uid')
    def _compute_description(self):
        self.check_access_rights('read')
        self.check_access_rule('read')

        is_officer = self.user_has_groups('hr_holidays.group_hr_holidays_user')

        for leave in self:
            if is_officer or leave.user_id == self.env.user or leave.employee_id.leave_manager_id == self.env.user:
                leave.name = leave.sudo().private_name
            else:
                leave.name = '*****'

    def _search_description(self, operator, value):
        is_officer = self.user_has_groups('hr_holidays.group_hr_holidays_user')
        domain = [('private_name', operator, value)]

        if not is_officer:
            domain = expression.AND([domain, [('user_id', '=', self.env.user.id)]])

        leaves = self.search(domain)
        return [('id', 'in', leaves.ids)]

    @api.depends('holiday_type')
    def _compute_from_holiday_type(self):
        allocation_from_domain = self.env['hr.leave.allocation']
        if (self._context.get('active_model') == 'hr.leave.allocation' and
           self._context.get('active_id')):
            allocation_from_domain = allocation_from_domain.browse(self._context['active_id'])
        for holiday in self:
            if holiday.holiday_type == 'employee':
                holiday.mode_company_id = False
                holiday.category_id = False
            elif holiday.holiday_type == 'company':
                if not holiday.mode_company_id:
                    holiday.mode_company_id = self.env.company.id
                holiday.category_id = False
            elif holiday.holiday_type == 'department':
                holiday.mode_company_id = False
                holiday.category_id = False
            elif holiday.holiday_type == 'category':
                holiday.mode_company_id = False

    @api.depends('employee_id')
    def _compute_from_employee_id(self):
        for holiday in self:
            holiday.manager_id = holiday.employee_id.parent_id.id
            if holiday.holiday_status_id.requires_allocation == 'no':
                continue
            if holiday.employee_id.user_id != self.env.user and holiday._origin.employee_id != holiday.employee_id:
                if holiday.employee_id and not holiday.holiday_status_id.with_context(employee_id=holiday.employee_id.id).has_valid_allocation:
                    holiday.holiday_status_id = False

    @api.depends('holiday_status_id')
    def _compute_state(self):
        for leave in self:
            leave.state = 'confirm' if leave.validation_type != 'no_validation' else 'draft'

    @api.depends('holiday_status_id.requires_allocation', 'validation_type', 'employee_id', 'date_from', 'date_to')
    def _compute_from_holiday_status_id(self):
        self.holiday_allocation_id = False

    @api.depends('request_date_from_period', 'request_hour_from', 'request_hour_to', 'request_date_from', 'request_date_to',
                 'request_unit_half', 'request_unit_hours', 'employee_id')
    def _compute_date_from_to(self):
        for holiday in self:
            if holiday.request_date_from and holiday.request_date_to and holiday.request_date_from > holiday.request_date_to:
                holiday.request_date_to = holiday.request_date_from
            if not holiday.request_date_from:
                holiday.date_from = False
            elif not holiday.request_unit_half and not holiday.request_unit_hours and not holiday.request_date_to:
                holiday.date_to = False
            else:
                if holiday.request_unit_half or holiday.request_unit_hours:
                    holiday.request_date_to = holiday.request_date_from

                attendance_from, attendance_to = holiday._get_attendances(holiday.employee_id, holiday.request_date_from, holiday.request_date_to)

                compensated_request_date_from = holiday.request_date_from
                compensated_request_date_to = holiday.request_date_to

                if holiday.request_unit_half:
                    if holiday.request_date_from_period == 'am':
                        hour_from = attendance_from.hour_from
                        hour_to = attendance_from.hour_to
                    else:
                        hour_from = attendance_to.hour_from
                        hour_to = attendance_to.hour_to
                elif holiday.request_unit_hours:
                    hour_from = holiday.request_hour_from
                    hour_to = holiday.request_hour_to
                else:
                    hour_from = attendance_from.hour_from
                    hour_to = attendance_to.hour_to

                holiday.date_from = self._get_start_or_end_from_attendance(hour_from, compensated_request_date_from, holiday.employee_id or holiday)
                holiday.date_to = self._get_start_or_end_from_attendance(hour_to, compensated_request_date_to, holiday.employee_id or holiday)

    @api.depends('holiday_status_id', 'request_unit_hours')
    def _compute_request_unit_half(self):
        for holiday in self:
            if holiday.holiday_status_id or holiday.request_unit_hours:
                holiday.request_unit_half = False

    @api.depends('holiday_status_id', 'request_unit_half')
    def _compute_request_unit_hours(self):
        for holiday in self:
            if holiday.holiday_status_id or holiday.request_unit_half:
                holiday.request_unit_hours = False

    @api.depends('employee_id', 'holiday_type')
    def _compute_department_id(self):
        for holiday in self:
            if holiday.employee_id:
                holiday.department_id = holiday.employee_id.department_id
            elif holiday.holiday_type == 'department':
                if not holiday.department_id:
                    holiday.department_id = self.env.user.employee_id.department_id
            else:
                holiday.department_id = False

    @api.depends('tz')
    @api.depends_context('uid')
    def _compute_tz_mismatch(self):
        for leave in self:
            leave.tz_mismatch = leave.tz != self.env.user.tz

    @api.depends('employee_id', 'holiday_type', 'department_id.company_id.resource_calendar_id.tz', 'mode_company_id.resource_calendar_id.tz')
    def _compute_tz(self):
        for leave in self:
            tz = False
            if leave.holiday_type == 'employee':
                tz = leave.employee_id.tz
            elif leave.holiday_type == 'department':
                tz = leave.department_id.company_id.resource_calendar_id.tz
            elif leave.holiday_type == 'company':
                tz = leave.mode_company_id.resource_calendar_id.tz
            leave.tz = tz or self.env.company.resource_calendar_id.tz or self.env.user.tz or 'UTC'

    def _get_number_of_days_batch(self, date_from, date_to, employee_id):
        """ Returns a float equals to the timedelta between two dates given as string."""
        employee = self.env['hr.employee'].browse(employee_id)
        # We force the company in the domain as we are more than likely in a compute_sudo
        domain = [('company_id', 'in', self.env.company.ids + self.env.context.get('allowed_company_ids', []))]

        result = employee._get_work_days_data_batch(date_from, date_to, domain=domain)
        for employee_id in result:
            if self.request_unit_half and result[employee_id]['hours'] > 0:
                result[employee_id]['days'] = 0.5
        return result

    def _get_number_of_days(self, date_from, date_to, employee_id):
        """ Returns a float equals to the timedelta between two dates given as string."""
        if employee_id:
            res = self._get_number_of_days_batch(date_from, date_to, employee_id)[employee_id]
            return res

        today_hours = self.env.company.resource_calendar_id.get_work_hours_count(
            datetime.combine(date_from.date(), time.min),
            datetime.combine(date_from.date(), time.max),
            False)

        hours = self.env.company.resource_calendar_id.get_work_hours_count(date_from, date_to)
        days = hours / (today_hours or HOURS_PER_DAY) if not self.request_unit_half else 0.5

        return {'days': days, 'hours': hours}

    @api.depends('date_from', 'date_to', 'employee_id')
    def _compute_number_of_days(self):
        for holiday in self:
            if holiday.date_from and holiday.date_to:
                holiday.number_of_days = holiday._get_number_of_days(holiday.date_from, holiday.date_to, holiday.employee_id.id)['days']
            else:
                holiday.number_of_days = 0

    @api.depends('number_of_days')
    def _compute_number_of_days_display(self):
        for holiday in self:
            holiday.number_of_days_display = holiday.number_of_days

    def _get_calendar(self):
        self.ensure_one()
        return self.employee_id.resource_calendar_id or self.env.company.resource_calendar_id

    @api.depends('number_of_days')
    def _compute_number_of_hours_display(self):
        for holiday in self:
            calendar = holiday._get_calendar()
            if holiday.date_from and holiday.date_to:
                # Take attendances into account, in case the leave validated
                # Otherwise, this will result into number_of_hours = 0
                # and number_of_hours_display = 0 or (#day * calendar.hours_per_day),
                # which could be wrong if the employee doesn't work the same number
                # hours each day
                if holiday.state == 'validate':
                    start_dt = holiday.date_from
                    end_dt = holiday.date_to
                    if not start_dt.tzinfo:
                        start_dt = start_dt.replace(tzinfo=UTC)
                    if not end_dt.tzinfo:
                        end_dt = end_dt.replace(tzinfo=UTC)
                    resource = holiday.employee_id.resource_id
                    intervals = calendar._attendance_intervals_batch(start_dt, end_dt, resource)[resource.id] \
                                - calendar._leave_intervals_batch(start_dt, end_dt, None)[False]  # Substract Global Leaves
                    number_of_hours = sum((stop - start).total_seconds() / 3600 for start, stop, dummy in intervals)
                else:
                    number_of_hours = holiday._get_number_of_days(holiday.date_from, holiday.date_to, holiday.employee_id.id)['hours']
                holiday.number_of_hours_display = number_of_hours or (holiday.number_of_days * (calendar.hours_per_day or HOURS_PER_DAY))
            else:
                holiday.number_of_hours_display = 0

    @api.depends('number_of_hours_display')
    def _compute_number_of_hours_text(self):
        # YTI Note: All this because a readonly field takes all the width on edit mode...
        for leave in self:
            leave.number_of_hours_text = '%s%g %s%s' % (
                '' if leave.request_unit_half or leave.request_unit_hours else '(',
                float_round(leave.number_of_hours_display, precision_digits=2),
                _('Hours'),
                '' if leave.request_unit_half or leave.request_unit_hours else ')')

    @api.depends('number_of_hours_display', 'number_of_days_display')
    def _compute_duration_display(self):
        for leave in self:
            leave.duration_display = '%g %s' % (
                (float_round(leave.number_of_hours_display, precision_digits=2)
                if leave.leave_type_request_unit == 'hour'
                else float_round(leave.number_of_days_display, precision_digits=2)),
                _('hours') if leave.leave_type_request_unit == 'hour' else _('days'))

    def _check_approval_update(self, state):
        """ Check if target state is achievable. """
        if self.env.is_superuser():
            return

        current_employee = self.env.user.employee_id
        is_officer = self.env.user.has_group('hr_holidays.group_hr_holidays_user')
        is_manager = self.env.user.has_group('hr_holidays.group_hr_holidays_manager')

        for holiday in self:
            val_type = holiday.validation_type

            if not is_manager and state != 'confirm':
                if state == 'draft':
                    if holiday.state == 'refuse':
                        raise UserError(_('Only a Time Off Manager can reset a refused leave.'))
                    if holiday.date_from and holiday.date_from.date() <= fields.Date.today():
                        raise UserError(_('Only a Time Off Manager can reset a started leave.'))
                    if holiday.employee_id != current_employee:
                        raise UserError(_('Only a Time Off Manager can reset other people leaves.'))
                else:
                    if val_type == 'no_validation' and current_employee == holiday.employee_id:
                        continue
                    # use ir.rule based first access check: department, members, ... (see security.xml)
                    holiday.check_access_rule('write')

                    # This handles states validate1 validate and refuse
                    if holiday.employee_id == current_employee:
                        raise UserError(_('Only a Time Off Manager can approve/refuse its own requests.'))

                    if (state == 'validate1' and val_type == 'both') and holiday.holiday_type == 'employee':
                        if not is_officer and self.env.user != holiday.employee_id.leave_manager_id:
                            raise UserError(_('You must be either %s\'s manager or Time off Manager to approve this leave') % (holiday.employee_id.name))

                    if (state == 'validate' and val_type == 'manager') and self.env.user != holiday.employee_id.leave_manager_id:
                        employee = holiday.employee_id
                        raise UserError(_('You must be %s\'s Manager to approve this leave', employee))

                    if not is_officer and (state == 'validate' and val_type == 'hr') and holiday.holiday_type == 'employee':
                        raise UserError(_('You must either be a Time off Officer or Time off Manager to approve this leave'))

    @api.depends('state', 'employee_id', 'department_id')
    def _compute_can_reset(self):
        for holiday in self:
            try:
                holiday._check_approval_update('draft')
            except (AccessError, UserError):
                holiday.can_reset = False
            else:
                holiday.can_reset = True

    @api.depends('state', 'employee_id', 'department_id')
    def _compute_can_approve(self):
        for holiday in self:
            try:
                if holiday.state == 'confirm' and holiday.validation_type == 'both':
                    holiday._check_approval_update('validate1')
                else:
                    holiday._check_approval_update('validate')
            except (AccessError, UserError):
                holiday.can_approve = False
            else:
                holiday.can_approve = True

    @api.depends('leave_type_support_document', 'attachment_ids')
    def _compute_supported_attachment_ids(self):
        for holiday in self:
            holiday.supported_attachment_ids = holiday.attachment_ids
            holiday.supported_attachment_ids_count = len(holiday.attachment_ids.ids)

    @api.depends('state')
    def _compute_is_hatched(self):
        for holiday in self:
            holiday.is_striked = holiday.state == 'refuse'
            holiday.is_hatched = holiday.state not in ['refuse', 'validate']

    @api.depends('date_from', 'date_to', 'holiday_status_id')
    def _compute_has_stress_day(self):
        date_from, date_to = min(self.mapped('date_from')), max(self.mapped('date_to'))
        if date_from and date_to:
            stress_days = self.employee_id._get_stress_days(
                date_from.date(),
                date_to.date())

            for leave in self:
                domain = [
                    ('start_date', '<=', leave.date_to.date()),
                    ('end_date', '>=', leave.date_from.date()),
                    '|',
                        ('resource_calendar_id', '=', False),
                        ('resource_calendar_id', '=', (leave.employee_id.resource_calendar_id or self.env.company.resource_calendar_id).id)
                ]

                if leave.holiday_status_id.company_id:
                    domain += [('company_id', '=', leave.holiday_status_id.company_id.id)]
                leave.has_stress_day = leave.date_from and leave.date_to and stress_days.filtered_domain(domain)
        else:
            self.has_stress_day = False

    @api.model
    def get_unusual_days(self, date_from, date_to=None):
        return self.env.user.employee_id.sudo(False)._get_unusual_days(date_from, date_to)

    def _get_start_or_end_from_attendance(self, hour, date, employee):
        hour = float_to_time(float(hour))
        holiday_tz = timezone(employee.tz or self.env.user.tz)
        return holiday_tz.localize(datetime.combine(date, hour)).astimezone(UTC).replace(tzinfo=None)

    def _get_attendances(self, employee, request_date_from, request_date_to):
        resource_calendar_id = employee.resource_calendar_id or self.env.company.resource_calendar_id
        domain = [('calendar_id', '=', resource_calendar_id.id), ('display_type', '=', False)]
        attendances = self.env['resource.calendar.attendance'].read_group(domain,
            ['ids:array_agg(id)', 'hour_from:min(hour_from)', 'hour_to:max(hour_to)',
             'week_type', 'dayofweek', 'day_period'],
            ['week_type', 'dayofweek', 'day_period'], lazy=False)

        # Must be sorted by dayofweek ASC and day_period DESC
        attendances = sorted([DummyAttendance(group['hour_from'], group['hour_to'], group['dayofweek'], group['day_period'], group['week_type']) for group in attendances], key=lambda att: (att.dayofweek, att.day_period != 'morning'))

        default_value = DummyAttendance(0, 0, 0, 'morning', False)

        if resource_calendar_id.two_weeks_calendar:
            # find week type of start_date
            start_week_type = self.env['resource.calendar.attendance'].get_week_type(request_date_from)
            attendance_actual_week = [att for att in attendances if att.week_type is False or int(att.week_type) == start_week_type]
            attendance_actual_next_week = [att for att in attendances if att.week_type is False or int(att.week_type) != start_week_type]
            # First, add days of actual week coming after date_from
            attendance_filtred = [att for att in attendance_actual_week if int(att.dayofweek) >= request_date_from.weekday()]
            # Second, add days of the other type of week
            attendance_filtred += list(attendance_actual_next_week)
            # Third, add days of actual week (to consider days that we have remove first because they coming before date_from)
            attendance_filtred += list(attendance_actual_week)
            end_week_type = self.env['resource.calendar.attendance'].get_week_type(request_date_to)
            attendance_actual_week = [att for att in attendances if att.week_type is False or int(att.week_type) == end_week_type]
            attendance_actual_next_week = [att for att in attendances if att.week_type is False or int(att.week_type) != end_week_type]
            attendance_filtred_reversed = list(reversed([att for att in attendance_actual_week if int(att.dayofweek) <= request_date_to.weekday()]))
            attendance_filtred_reversed += list(reversed(attendance_actual_next_week))
            attendance_filtred_reversed += list(reversed(attendance_actual_week))

            # find first attendance coming after first_day
            attendance_from = attendance_filtred[0]
            # find last attendance coming before last_day
            attendance_to = attendance_filtred_reversed[0]
        else:
            # find first attendance coming after first_day
            attendance_from = next((att for att in attendances if int(att.dayofweek) >= request_date_from.weekday()), attendances[0] if attendances else default_value)
            # find last attendance coming before last_day
            attendance_to = next((att for att in reversed(attendances) if int(att.dayofweek) <= request_date_to.weekday()), attendances[-1] if attendances else default_value)

        return (attendance_from, attendance_to)

    def _check_holidays_before_public(self):
        for draft in self:
            mapped_days = self.holiday_status_id.get_employees_days((draft.employee_id).ids, draft.date_from.date())

            leave_days = mapped_days[draft.employee_id.id][draft.holiday_status_id.id]
            if leave_days.get('max_leaves') == 0:
                raise ValidationError(_('The number of remaining time off is not sufficient for this time off type.\n'
                                        'Please also check the time off waiting for validation.'))

    def do_public(self):
        dict_create = {
            'holiday_status_id': self.holiday_status_id.id,
            'holiday_type': self.holiday_type,
            'user_id': self.user_id.id,
            'request_date_from_period': self.request_date_from_period,
            'request_hour_from': self.request_hour_from,
            'request_hour_to': self.request_hour_to,
            'request_date_from': self.request_date_from,
            'request_date_to': self.request_date_to,
            'date_from': self.date_from,
            'date_to': self.date_to,
            'employee_company_id': self.employee_company_id.id,
            'notes': self.notes,
            'meeting_id': self.meeting_id.id,
            'parent_id': self.parent_id.id,
            'linked_request_ids': self.linked_request_ids.id,
            'first_approver_id': self.first_approver_id.id,
            'second_approver_id': self.second_approver_id.id,
            'attachment_ids': self.attachment_ids.id,
        }

        if self.holiday_type == 'employee':
            dict_create.update(
                {'employee_id': self.employee_id.id if self.employee_id else self.env.user.employee_id.id})
        elif self.holiday_type == 'company':
            dict_create.update({'mode_company_id': self.mode_company_id.id})
        elif self.holiday_type == 'category':
            dict_create.update({'category_id': self.category_id.id})
        elif self.holiday_type == 'department':
            dict_create.update({'department_id': self.department_id.id})

        # checking the availability of free days
        self._check_holidays_before_public()

        result = self.env['hr.leave'].sudo().create(dict_create)
        self.hr_leave_id = result.id
        self.toggle_active()

    # actions

    def action_public_list(self):
        for leave in self:
            leave.do_public()

    # buttons

    def action_public_button(self):

        self.do_public()

        return {
            'name': _('All Time Off'),
            'type': 'ir.actions.act_window',
            'res_model': 'hr.leave.report.calendar',
            'view_mode': 'calendar',
            'view_id': self.env.ref('hr_holidays.hr_leave_report_calendar_view').id,
            'target': 'current',
        }
