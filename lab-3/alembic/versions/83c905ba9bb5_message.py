"""message

Revision ID: 83c905ba9bb5
Revises: 3cafd8bdcea1
Create Date: 2025-03-11 23:08:31.196560

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '83c905ba9bb5'
down_revision: Union[str, None] = '3cafd8bdcea1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
