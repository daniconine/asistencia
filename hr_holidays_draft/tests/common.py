# -*- coding: utf-8 -*-

from odoo.addons.mail.tests.common import mail_new_test_user
from odoo.tests import common

import logging


_logger = logging.getLogger(__name__)


class TestHrHolidaysDraftCommon(common.TransactionCase):

    @classmethod
    def setUpClass(cls):
        super(TestHrHolidaysDraftCommon, cls).setUpClass()
        _logger.info('********************** Start test **********************')

        department_id = cls.env['hr.department'].with_context(tracking_disable=True).create({
            'name': 'Research and development',
        })

        department_id_1 = cls.env['hr.department'].with_context(tracking_disable=True).create({
            'name': 'Research and development 1',
        })

        department_id_2 = cls.env['hr.department'].with_context(tracking_disable=True).create({
            'name': 'Research and development 2',
        })

        cls.users_employee = []
        for i in range(1, 11):
            user = mail_new_test_user(cls.env, login=f'user_employee{i}', groups='base.group_user')

            cls.env['hr.employee'].create({
                'name': user.name,
                'user_id': user.id,
                'department_id': department_id.id,
            })
            cls.users_employee.append(user)

        cls.users_employee_hour = []
        for i in range(1, 11):
            user = mail_new_test_user(cls.env, login=f'user_employee_hour{i}', groups='base.group_user')

            cls.env['hr.employee'].create({
                'name': user.name,
                'user_id': user.id,
                'department_id': department_id.id,
            })
            cls.users_employee_hour.append(user)

        cls.users_employee_hour_half = []
        for i in range(1, 11):
            user = mail_new_test_user(cls.env, login=f'user_employee_hour_half{i}', groups='base.group_user')

            cls.env['hr.employee'].create({
                'name': user.name,
                'user_id': user.id,
                'department_id': department_id.id,
            })
            cls.users_employee_hour_half.append(user)

        cls.users_company = []
        for i in range(1, 11):
            user = mail_new_test_user(cls.env, login=f'user_company{i}', groups='base.group_user')
            cls.env['hr.employee'].create({
                'name': user.name,
                'user_id': user.id,
                'department_id': department_id_1.id,
            })
            cls.users_company.append(user)

        cls.users_department = []
        for i in range(1, 11):
            user = mail_new_test_user(cls.env, login=f'user_department{i}', groups='base.group_user')
            cls.env['hr.employee'].create({
                'name': user.name,
                'user_id': user.id,
                'department_id': department_id_2.id,
            })
            cls.users_department.append(user)

        cls.users_category = []
        cls.hr_employee_category = cls.env['hr.employee.category'].create({
            'name': 'Category name',
        })
        for i in range(1, 11):
            user = mail_new_test_user(cls.env, login=f'user_category{i}', groups='base.group_user')
            cls.env['hr.employee'].create({
                'name': user.name,
                'user_id': user.id,
                'department_id': department_id.id,
                'category_ids': cls.hr_employee_category,
            })
            cls.users_category.append(user)

        cls.hr_leave_type_employee = cls.env['hr.leave.type'].create({
            'name': 'hr_leave_type_employee',
            'leave_validation_type': 'hr',
        })

        cls.hr_leave_type_employee_hour = cls.env['hr.leave.type'].create({
            'name': 'hr_leave_type_employee_hour',
            'leave_validation_type': 'hr',
            'request_unit': 'hour',
        })

        cls.hr_leave_type_company = cls.env['hr.leave.type'].create({
            'name': 'hr_leave_type_company',
            'leave_validation_type': 'hr',
        })

        cls.hr_leave_type_department = cls.env['hr.leave.type'].create({
            'name': 'hr_leave_type_department',
            'leave_validation_type': 'hr',
        })

        cls.hr_leave_type_category = cls.env['hr.leave.type'].create({
            'name': 'hr_leave_type_category',
            'leave_validation_type': 'hr',
        })

    @classmethod
    def tearDownClass(cls):
        super(TestHrHolidaysDraftCommon, cls).tearDownClass()
        _logger.info('********************** End test **********************')