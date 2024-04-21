#!/usr/bin/env python
# coding: utf-8

"""
LA Crime: Clear out the database
"""

from database_models import Base, engine

# Drop all tables
Base.metadata.drop_all(engine)

# Create all tables
Base.metadata.create_all(engine)