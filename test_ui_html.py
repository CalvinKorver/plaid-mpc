from ui import HTML


def test_rule_modal_buttons_have_visible_text():
    # Ensure cancel/back buttons in the rule modal use a dark text color
    assert "closeRuleModal()" in HTML
    assert "backToRuleEdit()" in HTML

    # Cancel button in step 1
    assert 'closeRuleModal()" style="padding:7px 14px;border-radius:6px;border:1px solid #ccc;background:#fff;color:#444;font-size:0.85rem;cursor:pointer"' in HTML

    # Back button in step 2
    assert 'backToRuleEdit()" style="padding:7px 14px;border-radius:6px;border:1px solid #ccc;background:#fff;color:#444;font-size:0.85rem;cursor:pointer"' in HTML

    # Cancel button in step 2
    assert 'closeRuleModal()" style="padding:7px 14px;border-radius:6px;border:1px solid #ccc;background:#fff;color:#444;font-size:0.85rem;cursor:pointer"' in HTML

