from odoo import models, fields, api
from datetime import datetime, timedelta, time

class HrEmployee(models.Model):
    _inherit = 'hr.employee'

    weekly_schedule_ids = fields.One2many('employee.weekly.schedule', 'employee_id', string="Horarios Semanales")

    def float_to_time(self, float_hour):
        """
        Convierte una hora en formato flotante (ej. 8.5 para 8:30 AM) a un objeto time.
        """
        hours = int(float_hour)
        minutes = int((float_hour - hours) * 60)
        return time(hours, minutes)

    def action_generate_schedule(self):
        """
        Genera los eventos de horario semanal en el calendario basándose en los horarios configurados para cada empleado.
        """

        # Mapeo de días de la semana
        day_mapping = {
            'monday': 'Lunes', 
            'tuesday': 'Martes', 
            'wednesday': 'Miércoles', 
            'thursday': 'Jueves', 
            'friday': 'Viernes', 
            'saturday': 'Sábado', 
            'sunday': 'Domingo'
        }

        for schedule in self.weekly_schedule_ids:
            current_date = schedule.start_date

            while current_date <= schedule.end_date:
                # Obtener el día de la semana del current_date (0=lunes, 1=martes, etc.)
                day_of_week_num = current_date.weekday()

                # Si es el día configurado en el horario semanal
                if day_of_week_num == list(day_mapping.keys()).index(schedule.day_of_week):
                    # Convertir las horas flotantes a `datetime.time`
                    start_time = self.float_to_time(schedule.start_time)
                    end_time = self.float_to_time(schedule.end_time)

                    # Crear las horas completas combinando la fecha y la hora
                    start_datetime = datetime.combine(current_date, start_time)
                    end_datetime = datetime.combine(current_date, end_time)

                    # Añadir 5 horas para corregir el desfase de zona horaria
                    start_datetime_fixed = start_datetime + timedelta(hours=5)
                    end_datetime_fixed = end_datetime + timedelta(hours=5)

                    # Crear el nombre del evento con el día en formato de texto
                    event_name = f"Horario: {day_mapping[schedule.day_of_week]} - {self.name} "

                    # Crear el evento en el calendario
                    self.env['calendar.event'].create({
                        'name': event_name,  
                        'start': start_datetime_fixed,  # Hora de inicio ajustada
                        'stop': end_datetime_fixed,  # Hora de finalización ajustada
                        'allday': False,
                        'location': schedule.location,
                        'employee_id': self.id,
                    })

                # Mover al siguiente día
                current_date += timedelta(days=1)


class EmployeeWeeklySchedule(models.Model):
    _name = 'employee.weekly.schedule'
    _description = 'Horario Semanal de Empleado'

    employee_id = fields.Many2one('hr.employee', string="Empleado", required=True)
    day_of_week = fields.Selection([
        ('monday', 'Lunes'),
        ('tuesday', 'Martes'),
        ('wednesday', 'Miércoles'),
        ('thursday', 'Jueves'),
        ('friday', 'Viernes'),
        ('saturday', 'Sábado'),
        ('sunday', 'Domingo'),
    ], string="Día de la Semana", required=True)

    start_time = fields.Float(string="Hora de Entrada", required=True, help="Hora de entrada en formato de 24 horas (ej. 9.5 para 9:30 AM)")
    end_time = fields.Float(string="Hora de Salida", required=True, help="Hora de salida en formato de 24 horas (ej. 17.5 para 5:30 PM)")
    
    location = fields.Selection([
        ('office', 'Presencial'),
        ('remote', 'Teletrabajo'),
    ], string="Ubicación", required=True)
    
    start_date = fields.Date(string="Fecha Inicial", required=True)
    end_date = fields.Date(string="Fecha Final", required=True)
