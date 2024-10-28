
from datetime import datetime
from dateutil.relativedelta import relativedelta

from odoo import tests
from .common import TestHrHolidaysDraftCommon


@tests.tagged('hr_holidays_draft')
class TestHrHolidaysDraftCreate(TestHrHolidaysDraftCommon):

    @classmethod
    def setUpClass(cls):
        super(TestHrHolidaysDraftCreate, cls).setUpClass()

        cls.leave_ids_employee = []

        for user in cls.users_employee:
            date_from = (datetime.today() - relativedelta(days=1))
            date_to = datetime.today()
            cls.env['hr.leave.allocation'].create({
                'name': '20 days allocation',
                'holiday_status_id': cls.hr_leave_type_employee.id,
                'number_of_days': 20,
                'employee_id': user.employee_id.id,
                'state': 'confirm',
                'date_from': date_from,
            }).action_validate()
            result = cls.env['hr_holidays_draft.leave'].with_user(user.id).create({
                'user_id': user.id,
                'request_date_from': (datetime.today() - relativedelta(days=1)),
                'request_date_to': datetime.today(),
                'date_from': date_from,
                'date_to': date_to,
                'employee_company_id': user.company_id.id,
                'holiday_status_id': cls.hr_leave_type_employee.id,
            })
            cls.leave_ids_employee.append(result.id)

        cls.leave_ids_employee_hour = []
        for user in cls.users_employee_hour:
            date_from = (datetime.today() - relativedelta(days=1))
            date_to = datetime.today()
            cls.env['hr.leave.allocation'].create({
                'name': '20 days allocation',
                'holiday_status_id': cls.hr_leave_type_employee_hour.id,
                'number_of_days': 20,
                'employee_id': user.employee_id.id,
                'state': 'confirm',
                'date_from': date_from,
            }).action_validate()
            result = cls.env['hr_holidays_draft.leave'].with_user(user.id).create({
                'user_id': user.id,
                'request_date_from': date_from,
                'request_date_to': date_to,
                'date_from': date_from,
                'date_to': date_to,
                'employee_company_id': user.company_id.id,
                'holiday_status_id': cls.hr_leave_type_employee_hour.id,
            })
            cls.leave_ids_employee_hour.append(result.id)

        cls.leave_ids_employee_hour_half = []
        for user in cls.users_employee_hour_half:
            date_from = (datetime.today() - relativedelta(days=1))
            date_to = datetime.today()
            cls.env['hr.leave.allocation'].create({
                'name': '20 days allocation',
                'holiday_status_id': cls.hr_leave_type_employee_hour.id,
                'number_of_days': 20,
                'employee_id': user.employee_id.id,
                'state': 'confirm',
                'date_from': date_from,
            }).action_validate()
            result = cls.env['hr_holidays_draft.leave'].with_user(user.id).create({
                'user_id': user.id,
                'request_date_from': date_from,
                'date_from': date_from,
                'date_to': date_to,
                'employee_company_id': user.company_id.id,
                'request_unit_half': True,
                'holiday_status_id': cls.hr_leave_type_employee_hour.id,
            })
            cls.leave_ids_employee_hour_half.append(result.id)

        cls.leave_ids_company = []
        for user in cls.users_company:
            date_from = (datetime.today() - relativedelta(days=1))
            date_to = datetime.today()
            cls.env['hr.leave.allocation'].create({
                'name': '20 days allocation',
                'holiday_status_id': cls.hr_leave_type_company.id,
                'number_of_days': 20,
                'employee_id': user.employee_id.id,
                'state': 'confirm',
                'date_from': date_from,
            }).action_validate()
            result = cls.env['hr_holidays_draft.leave'].with_user(user.id).create({
                'user_id': user.id,
                'request_date_from': date_from,
                'request_date_to': date_to,
                'date_from': date_from,
                'date_to': date_to,
                'employee_company_id': user.company_id.id,
                'holiday_status_id': cls.hr_leave_type_company.id,
                'holiday_type': "company",
            })
            cls.leave_ids_company.append(result.id)

        cls.leave_ids_department = []
        for user in cls.users_department:
            date_from = (datetime.today() - relativedelta(days=1))
            date_to = datetime.today()
            cls.env['hr.leave.allocation'].create({
                'name': '20 days allocation',
                'holiday_status_id': cls.hr_leave_type_department.id,
                'number_of_days': 20,
                'employee_id': user.employee_id.id,
                'state': 'confirm',
                'date_from': date_from,
            }).action_validate()
            result = cls.env['hr_holidays_draft.leave'].with_user(user.id).create({
                'user_id': user.id,
                'request_date_from': date_from,
                'request_date_to': date_to,
                'date_from': date_from,
                'date_to': date_to,
                'employee_company_id': user.company_id.id,
                'holiday_status_id': cls.hr_leave_type_department.id,
                'holiday_type': "department",
            })
            cls.leave_ids_department.append(result.id)

        cls.leave_ids_category = []
        for user in cls.users_category:
            date_from = (datetime.today() - relativedelta(days=1))
            date_to = datetime.today()
            cls.env['hr.leave.allocation'].create({
                'name': '20 days allocation',
                'holiday_status_id': cls.hr_leave_type_category.id,
                'number_of_days': 20,
                'employee_id': user.employee_id.id,
                'state': 'confirm',
                'date_from': date_from,
            }).action_validate()
            result = cls.env['hr_holidays_draft.leave'].with_user(user.id).create({
                'user_id': user.id,
                'request_date_from': date_from,
                'request_date_to': date_to,
                'date_from': date_from,
                'date_to': date_to,
                'employee_company_id': user.company_id.id,
                'holiday_status_id': cls.hr_leave_type_category.id,
                'holiday_type': "category",
                'category_id': cls.hr_employee_category.id,
            })
            cls.leave_ids_category.append(result.id)

    def test_create_publish_draft_employee(cls):
        cls.assertEqual(len(cls.leave_ids_employee), 10)

        for user in cls.users_employee:
            result = cls.env['hr_holidays_draft.leave'].search([('employee_id', '=', user.employee_id.id)])
            cls.assertEqual(len(result), 1)
            result = cls.env['hr_holidays_draft.leave'].with_user(user.id).search([])
            cls.assertEqual(len(result), 1)

            result.action_public_list()
            result = result.filtered(lambda x: x.active)
            cls.assertEqual(len(result), 0)

        leaves = cls.env['hr_holidays_draft.leave'].browse(cls.leave_ids_employee).filtered(lambda x: x.active)
        cls.assertEqual(len(leaves), 0)

    def test_create_publish_drafts_employee(cls):
        cls.assertEqual(len(cls.leave_ids_employee), 10)

        for user in cls.users_employee:
            result = cls.env['hr_holidays_draft.leave'].search([('employee_id', '=', user.employee_id.id)])
            cls.assertEqual(len(result), 1)
            result = cls.env['hr_holidays_draft.leave'].with_user(user.id).search([])
            cls.assertEqual(len(result), 1)

        leaves = cls.env['hr_holidays_draft.leave'].browse(cls.leave_ids_employee)
        cls.assertEqual(len(leaves), 10)
        leaves.action_public_list()
        leaves = leaves.filtered(lambda x: x.active)
        cls.assertEqual(len(leaves), 0)

    def test_create_publish_draft_employee_hour(cls):
        cls.assertEqual(len(cls.leave_ids_employee_hour), 10)

        for user in cls.users_employee_hour:
            result = cls.env['hr_holidays_draft.leave'].search([('employee_id', '=', user.employee_id.id)])
            cls.assertEqual(len(result), 1)
            result = cls.env['hr_holidays_draft.leave'].with_user(user.id).search([])
            cls.assertEqual(len(result), 1)

            result.action_public_list()
            result = result.filtered(lambda x: x.active)
            cls.assertEqual(len(result), 0)

        leaves = cls.env['hr_holidays_draft.leave'].browse(cls.leave_ids_employee_hour).filtered(lambda x: x.active)
        cls.assertEqual(len(leaves), 0)

    def test_create_publish_drafts_employee_hour(cls):
        cls.assertEqual(len(cls.leave_ids_employee_hour), 10)

        for user in cls.users_employee_hour:
            result = cls.env['hr_holidays_draft.leave'].search([('employee_id', '=', user.employee_id.id)])
            cls.assertEqual(len(result), 1)
            result = cls.env['hr_holidays_draft.leave'].with_user(user.id).search([])
            cls.assertEqual(len(result), 1)

        leaves = cls.env['hr_holidays_draft.leave'].browse(cls.leave_ids_employee_hour)
        cls.assertEqual(len(leaves), 10)
        leaves.action_public_list()
        leaves = leaves.filtered(lambda x: x.active)
        cls.assertEqual(len(leaves), 0)

    def test_create_publish_draft_employee_hour_half(cls):
        cls.assertEqual(len(cls.leave_ids_employee_hour_half), 10)

        for user in cls.users_employee_hour_half:
            result = cls.env['hr_holidays_draft.leave'].search([('employee_id', '=', user.employee_id.id)])
            cls.assertEqual(len(result), 1)
            result = cls.env['hr_holidays_draft.leave'].with_user(user.id).search([])
            cls.assertEqual(len(result), 1)

            result.action_public_list()
            result = result.filtered(lambda x: x.active)
            cls.assertEqual(len(result), 0)

        leaves = cls.env['hr_holidays_draft.leave'].browse(cls.leave_ids_employee_hour_half).filtered(lambda x: x.active)
        cls.assertEqual(len(leaves), 0)

    def test_create_publish_drafts_employee_hour_half(cls):
        cls.assertEqual(len(cls.leave_ids_employee_hour_half), 10)

        for user in cls.users_employee_hour_half:
            result = cls.env['hr_holidays_draft.leave'].search([('employee_id', '=', user.employee_id.id)])
            cls.assertEqual(len(result), 1)
            result = cls.env['hr_holidays_draft.leave'].with_user(user.id).search([])
            cls.assertEqual(len(result), 1)

        leaves = cls.env['hr_holidays_draft.leave'].browse(cls.leave_ids_employee_hour_half)
        cls.assertEqual(len(leaves), 10)
        leaves.action_public_list()
        leaves = leaves.filtered(lambda x: x.active)
        cls.assertEqual(len(leaves), 0)

    def test_create_publish_draft_company(cls):
        cls.assertEqual(len(cls.leave_ids_company), 10)

        for user in cls.users_company:
            result = cls.env['hr_holidays_draft.leave'].search([('employee_id', '=', user.employee_id.id)])
            cls.assertEqual(len(result), 1)
            result = cls.env['hr_holidays_draft.leave'].with_user(user.id).search([])
            cls.assertEqual(len(result), 1)

            result.action_public_list()
            result = result.filtered(lambda x: x.active)
            cls.assertEqual(len(result), 0)

        leaves = cls.env['hr_holidays_draft.leave'].browse(cls.leave_ids_company).filtered(lambda x: x.active)
        cls.assertEqual(len(leaves), 0)

    def test_create_publish_drafts_company(cls):
        cls.assertEqual(len(cls.leave_ids_company), 10)

        for user in cls.users_company:
            result = cls.env['hr_holidays_draft.leave'].search([('employee_id', '=', user.employee_id.id)])
            cls.assertEqual(len(result), 1)
            result = cls.env['hr_holidays_draft.leave'].with_user(user.id).search([])
            cls.assertEqual(len(result), 1)

        leaves = cls.env['hr_holidays_draft.leave'].browse(cls.leave_ids_company)
        cls.assertEqual(len(leaves), 10)
        leaves.action_public_list()
        leaves = leaves.filtered(lambda x: x.active)
        cls.assertEqual(len(leaves), 0)

    def test_create_publish_draft_department(cls):
        cls.assertEqual(len(cls.leave_ids_department), 10)

        for user in cls.users_department:
            result = cls.env['hr_holidays_draft.leave'].search([('employee_id', '=', user.employee_id.id)])
            cls.assertEqual(len(result), 1)
            result = cls.env['hr_holidays_draft.leave'].with_user(user.id).search([])
            cls.assertEqual(len(result), 1)

            result.action_public_list()
            result = result.filtered(lambda x: x.active)
            cls.assertEqual(len(result), 0)

        leaves = cls.env['hr_holidays_draft.leave'].browse(cls.leave_ids_department).filtered(lambda x: x.active)
        cls.assertEqual(len(leaves), 0)

    def test_create_publish_drafts_department(cls):
        cls.assertEqual(len(cls.leave_ids_department), 10)

        for user in cls.users_department:
            result = cls.env['hr_holidays_draft.leave'].search([('employee_id', '=', user.employee_id.id)])
            cls.assertEqual(len(result), 1)
            result = cls.env['hr_holidays_draft.leave'].with_user(user.id).search([])
            cls.assertEqual(len(result), 1)

        leaves = cls.env['hr_holidays_draft.leave'].browse(cls.leave_ids_department)
        cls.assertEqual(len(leaves), 10)
        leaves.action_public_list()
        leaves = leaves.filtered(lambda x: x.active)
        cls.assertEqual(len(leaves), 0)

    def test_create_publish_draft_category(cls):
        cls.assertEqual(len(cls.leave_ids_category), 10)

        for user in cls.users_category:
            result = cls.env['hr_holidays_draft.leave'].search([('employee_id', '=', user.employee_id.id)])
            cls.assertEqual(len(result), 1)
            result = cls.env['hr_holidays_draft.leave'].with_user(user.id).search([])
            cls.assertEqual(len(result), 1)

            result.action_public_list()
            result = result.filtered(lambda x: x.active)
            cls.assertEqual(len(result), 0)

        leaves = cls.env['hr_holidays_draft.leave'].browse(cls.leave_ids_category).filtered(lambda x: x.active)
        cls.assertEqual(len(leaves), 0)

    def test_create_publish_drafts_category(cls):
        cls.assertEqual(len(cls.leave_ids_category), 10)

        for user in cls.users_category:
            result = cls.env['hr_holidays_draft.leave'].search([('employee_id', '=', user.employee_id.id)])
            cls.assertEqual(len(result), 1)
            result = cls.env['hr_holidays_draft.leave'].with_user(user.id).search([])
            cls.assertEqual(len(result), 1)

        leaves = cls.env['hr_holidays_draft.leave'].browse(cls.leave_ids_category)
        cls.assertEqual(len(leaves), 10)
        leaves.action_public_list()
        leaves = leaves.filtered(lambda x: x.active)
        cls.assertEqual(len(leaves), 0)
