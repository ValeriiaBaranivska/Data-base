"""step-3

Revision ID: d25d6d6341b4
Revises: 83c905ba9bb5
Create Date: 2025-03-12 13:18:50.565146

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd25d6d6341b4'
down_revision: Union[str, None] = '83c905ba9bb5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
