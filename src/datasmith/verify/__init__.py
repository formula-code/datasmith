"""Task approval and repair: the per-task stages behind the ``fc-verify`` command.

A survey folder holds one state file per task (``state/<task>.json``), the oracle records (``oracle/<task>.json``),
the pinned task folders (``pinned/<task>/``), the noise rule (``threshold_rule.json``) and the review file
(``manual_review.csv``).
"""
