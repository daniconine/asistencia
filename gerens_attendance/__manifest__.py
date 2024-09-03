# Módulo de Desarrollo
# Copyright 2014-2015 Grupo ESOC <www.grupoesoc.es>
# Copyright 2017-Apertoso N.V. (<http://www.apertoso.be>)
# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl.html).
{
    "name": "Asistencia Gerens Desarrollo",
    "summary": "Add features to Attendance ",
    "version": "15.0.1.0.0",
    "category": "Customer Relationship Management",
    "website": "",
    "author": "Daniel C, Grupo ESOC, Tecnativa, Odoo Community Association (OCA)",
    "license": "AGPL-3",
    "application": False,
    "installable": True,
    "auto_install": False,
    "depends": ["hr_attendance"],
    "data": [
             "views/attendance_gerens_view.xml",
             "views/calendar_event_view.xml",
             "views/attendance_custom_views.xml",
             ],
    #"post_init_hook": "post_init_hook",
}

#"data/tardanza_email_template.xml",