"""step-3-check

Revision ID: a4d2fbdae926
Revises: d25d6d6341b4
Create Date: 2025-03-12 14:04:17.355257

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a4d2fbdae926'
down_revision: Union[str, None] = 'd25d6d6341b4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
