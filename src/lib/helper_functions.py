#!/usr/bin/env python
"""
Add a description here
"""

#built in python modules
from datetime import datetime

def _getToday():
		"""Create timestamp string"""
		return datetime.now().strftime('%Y%m%d%H%M%S')