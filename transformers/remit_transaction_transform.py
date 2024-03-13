from mage_ai.data_cleaner.transformer_actions.base import BaseAction
from mage_ai.data_cleaner.transformer_actions.constants import ActionType, Axis
from mage_ai.data_cleaner.transformer_actions.utils import build_transformer_action
from pandas import DataFrame

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def execute_transformer_action(df: DataFrame, *args, **kwargs) -> DataFrame:
    """
    Execute Transformer Action: ActionType.REMOVE

    Docs: https://docs.mage.ai/guides/transformer-blocks#remove-columns
    """
    action = build_transformer_action(
        df,
        action_type=ActionType.REMOVE,
        arguments=['app_id','received_currency', 'sender_funding_account_id', 'ip', 'purpose_of_remittance', 'reference_num', 'status_history','hold_reason','notes','remarks','payout_message','est_delivery_time','msb_reference_num','payout_reference_id','payout_batch_id','transaction_note','payment_detail','suspicious_detail'],  # Specify columns to remove
        axis=Axis.COLUMN,
    )

    return BaseAction(action).execute(df)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
