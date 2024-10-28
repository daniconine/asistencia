# -*- coding: utf-8 -*-
{
    'name': "HR Holiday Time Off Drafts",

    'summary': """HR Holiday Time Off Drafts""",

    'description': """
        HR Holiday Time Off Drafts
    """,

    'author': "Kub3",
    'website': "https://www.kub3.ru/",

    # for the full list
    'category': 'Human Resources/Time Off',
    'version': '1.0',

    # info for Odoo AppStore
    'price': 0,
    'currency': 'EUR',
    'support': 'info@kub3.pro',
    'images': ['static/description/main_screenshot.png'],


    # any module necessary for this one to work correctly
    'depends': ['base', 'hr_holidays'],

    # always loaded
    'data': [
        'security/hr_holidays_security.xml',
        'security/ir.model.access.csv',
        'views/views.xml',
    ],

    'assets': {
        'web.assets_backend': [
            'hr_holidays_draft/static/src/views/calendar/calendar_controller.xml',
            'hr_holidays_draft/static/src/views/calendar/calendar_controller.js',
            'hr_holidays_draft/static/src/views/hooks.js',

            'hr_holidays_draft/static/src/views/calendar/year/calendar_year_popover.xml',
            'hr_holidays_draft/static/src/views/calendar/year/calendar_year_popover.js',
            'hr_holidays_draft/static/src/views/calendar/year/calendar_year_renderer.js',
            'hr_holidays_draft/static/src/views/calendar/common/calendar_common_popover.js',
            'hr_holidays_draft/static/src/views/calendar/common/calendar_common_renderer.js',
            'hr_holidays_draft/static/src/dashboard/time_off_card.xml',
            'hr_holidays_draft/static/src/dashboard/time_off_card.js',
            'hr_holidays_draft/static/src/dashboard/time_off_dashboard.xml',
            'hr_holidays_draft/static/src/dashboard/time_off_dashboard.js',

            'hr_holidays_draft/static/src/views/calendar/calendar_renderer.xml',
            'hr_holidays_draft/static/src/views/calendar/calendar_renderer.js',

            'hr_holidays_draft/static/src/views/calendar/calendar_model.js',
            'hr_holidays_draft/static/src/views/calendar/calendar_view.js',
        ],
    },
    'installable': True,
    'application': True,
    'license': 'LGPL-3',
}
