#######################################################################################
#
#    Copyright (C) 2019-TODAY OPeru.
#    Author      :  Grupo Odoo S.A.C. (<http://www.operu.pe>)
#
#    This program is copyright property of the author mentioned above.
#    You can`t redistribute it and/or modify it.
#
#######################################################################################

from odoo import models, api, fields


class SaleOrder(models.Model):
    _inherit = "sale.order"

    def action_confirm(self):
        super(SaleOrder, self).action_confirm()
        self.update_tax_order_line()

    def action_open_reward_wizard(self):
        super(SaleOrder, self).action_open_reward_wizard()
        self.update_tax_order_line()

    def update_tax_order_line(self):
        for line in self.order_line:
            if (
                line.display_type not in ["line_section","line_note"]
                and line.l10n_pe_edi_is_free_product
            ):
                line.write({'discount': 100})
                line._compute_tax_id()
                
