# -*- coding: utf-8 -*-
{
'name': 'Show Duration in Leave Allocation',
'summary': 'Show Duration in Leave Allocation, Leave, Timeoff, Allocation, HR, Human '
           'Resources, Management, Payroll, Rules, Salary Rules, Payslip, Openinside, '
           'Odoo',
'version': '16.0.0.0.0',
'category': 'Human Resources',
'website': 'https://www.open-inside.com',
'description': '''
'''
               '''		* Show Duration in Leave Allocation
'''
               '		* get_leave_balance method in hr.employee to get balance for specific '
               '''date to use it in payroll rules
'''
               '    ',
'images': ['static/description/cover.png'],
'author': 'Openinside',
'license': 'OPL-1',
'price': 0.0,
'currency': 'USD',
'installable': True,
'depends': ['hr_holidays'],
'data': ['view/hr_leave_allocation.xml'],
'odoo-apps': True,
'application': False
}