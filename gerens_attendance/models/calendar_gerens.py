from odoo import models, fields

class CalendarEvent(models.Model):
    _inherit = 'calendar.event'

    employee_id = fields.Many2one('hr.employee', string="Empleado", help="Empleado relacionado con este evento")
