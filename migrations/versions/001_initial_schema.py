"""Initial schema for MVR crime bulletins

Revision ID: 001_initial_schema
Revises:
Create Date: 2026-04-08

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '001_initial_schema'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create bulletins table
    op.create_table(
        'bulletins',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('url', sa.String(length=500), nullable=False),
        sa.Column('publication_date', sa.Date(), nullable=True),
        sa.Column('raw_text', sa.Text(), nullable=True),
        sa.Column('processed_at', sa.DateTime(), nullable=True),
        sa.Column('status', sa.String(length=20), nullable=False, default='pending'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('url'),
    )
    op.create_index(op.f('ix_bulletins_url'), 'bulletins', ['url'], unique=True)

    # Create crime_incidents table
    op.create_table(
        'crime_incidents',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('bulletin_id', sa.Integer(), nullable=False),
        sa.Column('crime_type', sa.Text(), nullable=False),
        sa.Column('crime_date', sa.Date(), nullable=True),
        sa.Column('location_city', sa.Text(), nullable=False),
        sa.Column('location_address', sa.Text(), nullable=True),
        sa.Column('perpetrator_count', sa.String(length=20), nullable=False),
        sa.Column('perpetrator_ages', sa.JSON(), nullable=False),
        sa.Column('perpetrator_gender', sa.String(length=20), nullable=False),
        sa.Column('outcome', sa.Text(), nullable=True),
        sa.Column('raw_text', sa.Text(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['bulletin_id'], ['bulletins.id'], ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(op.f('ix_crime_incidents_bulletin_id'), 'crime_incidents', ['bulletin_id'], unique=False)

    # Create processing_errors table
    op.create_table(
        'processing_errors',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('bulletin_id', sa.Integer(), nullable=True),
        sa.Column('error_type', sa.String(length=100), nullable=False),
        sa.Column('error_detail', sa.Text(), nullable=True),
        sa.Column('raw_llm_output', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['bulletin_id'], ['bulletins.id'], ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(op.f('ix_processing_errors_bulletin_id'), 'processing_errors', ['bulletin_id'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_processing_errors_bulletin_id'), table_name='processing_errors')
    op.drop_table('processing_errors')
    op.drop_index(op.f('ix_crime_incidents_bulletin_id'), table_name='crime_incidents')
    op.drop_table('crime_incidents')
    op.drop_index(op.f('ix_bulletins_url'), table_name='bulletins')
    op.drop_table('bulletins')
