from odoo import http

class MyCustomController(http.Controller):
    @http.route('/my_endpoint', type='http', auth='user')
    def my_endpoint(self, **kwargs):
        # Aquí puedes agregar la lógica para manejar la solicitud
        return "¡Hola desde mi endpoint personalizado!"
