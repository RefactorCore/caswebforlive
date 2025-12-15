from flask import Blueprint, request, flash, redirect, url_for
from models import db, User
from passlib.hash import pbkdf2_sha256
from flask_login import login_required, current_user
from .decorators import role_required
from .utils import log_action
from sqlalchemy import func

user_bp = Blueprint('users', __name__, url_prefix='/users')

@user_bp.route('/create', methods=['POST'])
@login_required
@role_required('Admin')
def create_user():
    username = (request.form.get('username') or '').strip()
    password = request.form.get('password') or ''
    role = (request.form.get('role') or '').strip()

    # Basic validation
    if not username or not password or not role:
        flash('All fields are required.', 'danger')
        return redirect(url_for('core.settings'))

    if len(username) > 100 or len(password) > 200:
        flash('Username or password is too long.', 'danger')
        return redirect(url_for('core.settings'))

    # Prevent duplicate usernames (case-insensitive check)
    existing = User.query.filter(func.lower(User.username) == username.lower()).first()
    if existing:
        flash(f'Username "{username}" already exists.', 'danger')
        return redirect(url_for('core.settings'))

    try:
        new_user = User(
            username=username,
            password_hash=pbkdf2_sha256.hash(password),
            role=role
        )
        db.session.add(new_user)
        db.session.commit()
        log_action(f'Created new user: {username} with role: {role}.')
        flash(f'User "{username}" created successfully.', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Error creating user: {str(e)}', 'danger')

    return redirect(url_for('core.settings'))


@user_bp.route('/update/<int:user_id>', methods=['POST'])
@login_required
@role_required('Admin')
def update_user(user_id):
    user = User.query.get_or_404(user_id)
    new_password = request.form.get('password')
    role = (request.form.get('role') or '').strip()

    if not role:
        flash('Role is required.', 'danger')
        return redirect(url_for('core.settings'))

    try:
        user.role = role

        if new_password:
            if len(new_password) < 6:
                flash('Password must be at least 6 characters.', 'danger')
                return redirect(url_for('core.settings'))
            user.password_hash = pbkdf2_sha256.hash(new_password)

        db.session.commit()
        log_action(f'Updated user: {user.username}. Changed role to {role}.')
        flash(f'User "{user.username}" updated successfully.', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Error updating user: {str(e)}', 'danger')

    return redirect(url_for('core.settings'))


@user_bp.route('/delete/<int:user_id>', methods=['POST'])
@login_required
@role_required('Admin')
def delete_user(user_id):
    user = User.query.get_or_404(user_id)

    # Safety check: prevent a user from deleting themselves
    if user.id == current_user.id:
        flash('You cannot delete your own account.', 'danger')
        return redirect(url_for('core.settings'))

    try:
        # Prevent removing the last Admin account
        if user.role and user.role.lower() == 'admin':
            admin_count = User.query.filter(func.lower(User.role) == 'admin').count()
            if admin_count <= 1:
                flash('Cannot delete the last admin account.', 'danger')
                return redirect(url_for('core.settings'))

        db.session.delete(user)
        db.session.commit()
        log_action(f'Deleted user: {user.username}.')
        flash(f'User "{user.username}" has been deleted.', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Error deleting user: {str(e)}', 'danger')

    return redirect(url_for('core.settings'))