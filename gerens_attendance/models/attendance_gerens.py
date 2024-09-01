from odoo import api, fields, models
from odoo.tools import DEFAULT_SERVER_DATETIME_FORMAT
from pytz import timezone, UTC

class HrAttendance(models.Model):
    _inherit = 'hr.attendance'

    weekday = fields.Char(string='Dia', compute='_compute_weekday', store=True)
    entry_time = fields.Char(string='Hora entrada', compute='_compute_times', store=True)
    exit_time = fields.Char(string='Hora Salida', compute='_compute_times', store=True)

    @api.depends('check_in')
    def _compute_weekday(self):
        for record in self:
            if record.check_in:
                check_in_datetime = fields.Datetime.from_string(record.check_in)
                user_tz = self.env.user.tz or 'UTC'
                local_tz = timezone(user_tz)
                check_in_local = check_in_datetime.astimezone(local_tz)
                record.weekday = check_in_local.strftime('%A')
            else:
                record.weekday = False

    @api.depends('check_in', 'check_out')
    def _compute_times(self):
        for record in self:
            user_tz = self.env.user.tz or 'UTC'
            local_tz = timezone(user_tz)

            if record.check_in:
                check_in_datetime = fields.Datetime.from_string(record.check_in)
                check_in_local = check_in_datetime.astimezone(local_tz)
                record.entry_time = check_in_local.strftime('%H:%M:%S')
            else:
                record.entry_time = False

            if record.check_out:
                check_out_datetime = fields.Datetime.from_string(record.check_out)
                check_out_local = check_out_datetime.astimezone(local_tz)
                record.exit_time = check_out_local.strftime('%H:%M:%S')
            else:
                record.exit_time = False
