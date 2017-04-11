from sanic.response import json, redirect

from porcupine import App
from porcupine import db, context_user


class Auth(App):
    """Handles the users' authentication"""
    name = 'auth'

auth = Auth()


@auth.route('/', methods=frozenset({'POST'}))
async def auth_handler(request):
    user_name = request.json['name']
    password = request.json['password']
    async with context_user('system'):
        users = await db.get_item('users', quiet=False)
        user = await users.get_child_by_name(user_name)
        if user and hasattr(user, 'authenticate'):
            if user.authenticate(password):
                request.session['uid'] = user.id
                return json(True)
        return json(False)


@auth.route('/logout')
async def logout(request):
    request.session.terminate()
    if 'redirect' in request.args:
        return redirect(request.args['redirect'][0])
    return json(True)
