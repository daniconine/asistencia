from odoo import api, fields, models
from pytz import timezone

class HrAttendance(models.Model):
    _inherit = 'hr.attendance'

    weekday = fields.Char(string='Día', compute='_compute_weekday', store=True)
    entry_time = fields.Char(string='Hora de entrada', compute='_compute_times', store=True)
    exit_time = fields.Char(string='Hora de salida', compute='_compute_times', store=True)
    tardanza_minutos = fields.Integer(string="Minutos de Tardanza", compute="_compute_tardanza_minutos", store=True)
    horas_faltantes = fields.Float(string="Horas Faltantes/Extras", compute="_compute_horas_faltantes", store=True)

    @api.depends('check_in')
    def _compute_weekday(self):
        for record in self:
            if record.check_in:
                check_in_datetime = fields.Datetime.context_timestamp(record, record.check_in)
                record.weekday = check_in_datetime.strftime('%A')
            else:
                record.weekday = False

    @api.depends('check_in', 'check_out')
    def _compute_times(self):
        for record in self:
            if record.check_in:
                check_in_local = fields.Datetime.context_timestamp(record, record.check_in)
                record.entry_time = check_in_local.strftime('%H:%M:%S')
            else:
                record.entry_time = False

            if record.check_out:
                check_out_local = fields.Datetime.context_timestamp(record, record.check_out)
                record.exit_time = check_out_local.strftime('%H:%M:%S')
            else:
                record.exit_time = False

    @api.depends('check_in', 'employee_id')
    def _compute_tardanza_minutos(self):
        for record in self:
            record.tardanza_minutos = 0  # Valor predeterminado

            if record.check_in and record.employee_id:
                # Busca el evento del calendario que coincide con el empleado y la fecha/hora de check-in
                calendar_event = self.env['calendar.event'].search([
                    ('employee_id', '=', record.employee_id.id),
                    ('start', '<=', record.check_in),
                    ('stop', '>=', record.check_in),
                ], limit=1)

                if calendar_event:
                    expected_start = fields.Datetime.context_timestamp(record, calendar_event.start)
                    actual_start = fields.Datetime.context_timestamp(record, record.check_in)

                    if actual_start > expected_start:
                        tardanza = (actual_start - expected_start).total_seconds() / 60
                        record.tardanza_minutos = int(tardanza)

                        
    @api.depends('check_in', 'check_out', 'employee_id')
    def _compute_horas_faltantes(self):
        for record in self:
            record.horas_faltantes = 0.0  # Valor predeterminado

            if record.check_in and record.check_out and record.employee_id:
                # Busca el evento del calendario que coincide con el empleado y la fecha/hora de check-in
                calendar_event = self.env['calendar.event'].search([
                    ('employee_id', '=', record.employee_id.id),
                    ('start', '<=', record.check_in),
                    ('stop', '>=', record.check_in),
                ], limit=1)

                if calendar_event:
                    expected_start = fields.Datetime.context_timestamp(record, calendar_event.start)
                    expected_end = fields.Datetime.context_timestamp(record, calendar_event.stop)
                    actual_start = fields.Datetime.context_timestamp(record, record.check_in)
                    actual_end = fields.Datetime.context_timestamp(record, record.check_out)

                    # Calcular las horas programadas y las horas trabajadas
                    horas_programadas = (expected_end - expected_start).total_seconds() / 3600
                    horas_trabajadas = (actual_end - actual_start).total_seconds() / 3600

                    # Calcular la diferencia de horas
                    record.horas_faltantes = horas_trabajadas - horas_programadas

                    # Si la diferencia es negativa, significa que faltan horas
                    if record.horas_faltantes < 0:
                        record.horas_faltantes = round(record.horas_faltantes, 2)
                    else:
                        record.horas_faltantes = round(record.horas_faltantes, 2)