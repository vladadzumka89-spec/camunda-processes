#!/usr/bin/env python3
"""Generate merged BPMN: Сповіщення + Зміна ФОП на терміналі."""

WEBHOOK = "https://odoo.dev.dobrom.com/web/hook/90fdde6b-47f9-44ba-90b2-19559b206bce"
PROJECT_ID = 236

def q(s):
    """XML-escape quotes for attribute values."""
    return s.replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;').replace('&', '&amp;') if '&' not in s else s

def ut_xml(uid, name, body_expr, incoming, outgoing, outputs=None, task_listener=True):
    """Generate User Task XML."""
    inc = "\n".join(f'      <bpmn:incoming>{f}</bpmn:incoming>' for f in (incoming if isinstance(incoming, list) else [incoming]))
    out_flows = "\n".join(f'      <bpmn:outgoing>{f}</bpmn:outgoing>' for f in (outgoing if isinstance(outgoing, list) else [outgoing]))

    out_mappings = ""
    if outputs:
        for src, tgt in outputs.items():
            out_mappings += f'\n          <zeebe:output source="= {src}" target="{tgt}" />'

    listener = ""
    if task_listener:
        listener = """
        <zeebe:taskListeners>
          <zeebe:taskListener eventType="creating" type="http-request-smart" retries="1" />
        </zeebe:taskListeners>"""

    return f'''    <bpmn:userTask id="{uid}" name="{name}">
      <bpmn:extensionElements>
        <zeebe:ioMapping>
          <zeebe:input source="= &quot;POST&quot;" target="method" />
          <zeebe:input source="= &quot;{WEBHOOK}&quot;" target="url" />
          <zeebe:input source="= {{&quot;Content-Type&quot;:&quot;application/json&quot;}}" target="headers" />
          <zeebe:input source="= {body_expr}" target="body" />{out_mappings}
        </zeebe:ioMapping>
        <zeebe:userTask />{listener}
      </bpmn:extensionElements>
{inc}
{out_flows}
    </bpmn:userTask>'''


def body(name_expr, desc_expr, extra_fields=""):
    """Build body FEEL expression for Odoo webhook."""
    extra = f", {extra_fields}" if extra_fields else ""
    return (f'{{name: {name_expr}, description: {desc_expr}, '
            f'_model: &quot;project.project&quot;, _id: {PROJECT_ID}, '
            f'process_instance_key: process_instance_key{extra}}}')


def body_with_fop_fields(name_expr, desc_expr, extra=""):
    """Body with standard x_studio_camunda fields from Phase 1."""
    fields = (
        "x_studio_camunda_fop_name: fop_name, "
        "x_studio_camunda_total_income: total_income, "
        "x_studio_camunda_days_to_limit: days_to_limit"
    )
    if extra:
        fields += f", {extra}"
    return body(name_expr, desc_expr, fields)


# ============================================================
# PHASE 1 TASKS (from file 2)
# ============================================================

phase1_tasks = []

# UT_check_stores
phase1_tasks.append(ut_xml(
    "UT_check_stores",
    "Перевірити кількість магазинів на ФОП",
    body_with_fop_fields(
        '&quot;Перевірити магазини ФОП &quot; + fop_name',
        '&quot;&lt;b&gt;ФОП:&lt;/b&gt; &quot; + fop_name + &quot; (ЄДРПОУ: &quot; + fop_edrpou + &quot;)&lt;br/&gt;&lt;b&gt;Група ЄП:&lt;/b&gt; &quot; + string(ep_group) + &quot;&lt;br/&gt;&lt;b&gt;Дохід за рік:&lt;/b&gt; &quot; + string(total_income) + &quot; грн (&quot; + string(income_percent) + &quot;% від ліміту &quot; + string(limit_amount) + &quot; грн)&lt;br/&gt;&lt;b&gt;Днів до ліміту:&lt;/b&gt; &quot; + string(days_to_limit) + &quot;&lt;br/&gt;&lt;b&gt;Прогнозна дата:&lt;/b&gt; &quot; + projected_date + &quot;&lt;br/&gt;&lt;b&gt;Магазини:&lt;/b&gt; &quot; + stores + &quot;&lt;br/&gt;&lt;br/&gt;Перевірте кількість магазинів та підтвердіть.&quot;'
    ),
    ["Flow_skip_create", "Flow_create_to_merge"],
    "Flow_stores_to_select"
))

# UT_select_store
phase1_tasks.append(ut_xml(
    "UT_select_store",
    "Вибрати магазин, який переключаємо",
    body_with_fop_fields(
        '&quot;Вибрати магазин для переключення ФОП &quot; + fop_name',
        '&quot;&lt;b&gt;ФОП:&lt;/b&gt; &quot; + fop_name + &quot; (&quot; + string(income_percent) + &quot;% ліміту, &quot; + string(days_to_limit) + &quot; днів до ліміту)&lt;br/&gt;&lt;b&gt;Магазини:&lt;/b&gt; &quot; + stores + &quot;&lt;br/&gt;&lt;br/&gt;Оберіть магазин який переключаємо на іншу ФОП.&lt;br/&gt;Впишіть назву магазину у поле «Обраний магазин».&quot;'
    ),
    "Flow_stores_to_select",
    "Flow_select_to_paid",
    outputs={"x_studio_camunda_selected_store": "selected_store"}
))

# UT_check_paid
phase1_tasks.append(ut_xml(
    "UT_check_paid",
    "Перевірити чи платна зміна ФОП",
    body_with_fop_fields(
        '&quot;Перевірити чи платна зміна ФОП на &quot; + selected_store',
        '&quot;&lt;b&gt;Магазин:&lt;/b&gt; &quot; + selected_store + &quot;&lt;br/&gt;&lt;b&gt;ФОП:&lt;/b&gt; &quot; + fop_name + &quot; (дохід &quot; + string(total_income) + &quot; грн, &quot; + string(income_percent) + &quot;% ліміту)&lt;br/&gt;&lt;br/&gt;Перевірте чи ТРЦ бере плату за зміну ФОП на терміналі.&lt;br/&gt;Поставте галочку у полі «Чи платна зміна ФОП» якщо зміна платна.&quot;',
        'x_studio_camunda_selected_store: selected_store'
    ),
    "Flow_select_to_paid",
    "Flow_paid_to_notify",
    outputs={"x_studio_camunda_is_paid_change": "is_paid_change"}
))

# UT_notify_trc
phase1_tasks.append(ut_xml(
    "UT_notify_trc",
    "Сповістити ТРЦ про зміну ФОП з вказаною датою переключення",
    body_with_fop_fields(
        '&quot;Сповістити ТРЦ про зміну ФОП на &quot; + selected_store',
        '&quot;&lt;b&gt;Магазин:&lt;/b&gt; &quot; + selected_store + &quot;&lt;br/&gt;&lt;b&gt;Стара ФОП:&lt;/b&gt; &quot; + fop_name + &quot; (ЄДРПОУ: &quot; + fop_edrpou + &quot;)&lt;br/&gt;&lt;b&gt;Група ЄП:&lt;/b&gt; &quot; + string(ep_group) + &quot;&lt;br/&gt;&lt;b&gt;Дохід:&lt;/b&gt; &quot; + string(total_income) + &quot; грн (&quot; + string(income_percent) + &quot;% ліміту)&lt;br/&gt;&lt;b&gt;Днів до ліміту:&lt;/b&gt; &quot; + string(days_to_limit) + &quot;&lt;br/&gt;&lt;br/&gt;Зверніться до ТРЦ з питанням зміни ФОП на терміналі.&lt;br/&gt;Вкажіть дату переключення.&quot;',
        'x_studio_camunda_selected_store: selected_store, x_studio_camunda_is_paid_change: (if is defined(is_paid_change) then is_paid_change else false)'
    ),
    "Flow_paid_to_notify",
    "Flow_notify_to_gw_paid"
))

# UT_pay_change
phase1_tasks.append(ut_xml(
    "UT_pay_change",
    "Оплатити зміну ФОП",
    body_with_fop_fields(
        '&quot;Оплатити зміну ФОП на &quot; + selected_store',
        '&quot;&lt;b&gt;Магазин:&lt;/b&gt; &quot; + selected_store + &quot;&lt;br/&gt;&lt;b&gt;Стара ФОП:&lt;/b&gt; &quot; + fop_name + &quot;&lt;br/&gt;&lt;b&gt;Дохід:&lt;/b&gt; &quot; + string(total_income) + &quot; грн (&quot; + string(income_percent) + &quot;% ліміту)&lt;br/&gt;&lt;br/&gt;Необхідно оплатити зміну ФОП на терміналі ТРЦ.&quot;',
        'x_studio_camunda_selected_store: selected_store, x_studio_camunda_is_paid_change: (if is defined(is_paid_change) then is_paid_change else false)'
    ),
    "Flow_yes_paid",
    "Flow_pay_to_merge"
))

# UT_get_new_fop
phase1_tasks.append(ut_xml(
    "UT_get_new_fop",
    "Беремо ФОП з 0 оборотом",
    body_with_fop_fields(
        '&quot;Знайти ФОП з 0 оборотом для заміни &quot; + fop_name',
        '&quot;&lt;b&gt;Магазин:&lt;/b&gt; &quot; + selected_store + &quot;&lt;br/&gt;&lt;b&gt;Стара ФОП:&lt;/b&gt; &quot; + fop_name + &quot; (ЄДРПОУ: &quot; + fop_edrpou + &quot;, група &quot; + string(ep_group) + &quot;)&lt;br/&gt;&lt;b&gt;Дохід:&lt;/b&gt; &quot; + string(total_income) + &quot; грн (&quot; + string(income_percent) + &quot;% ліміту)&lt;br/&gt;&lt;br/&gt;Необхідно підібрати ФОП з нульовим оборотом для переключення терміналу.&lt;br/&gt;Впишіть назву нової ФОП у поле «Новий ФОП (з 0 оборотом)».&quot;',
        'x_studio_camunda_selected_store: selected_store, x_studio_camunda_is_paid_change: (if is defined(is_paid_change) then is_paid_change else false)'
    ),
    ["Flow_pay_to_merge", "Flow_not_paid"],
    "Flow_fop_to_parallel",
    outputs={"x_studio_camunda_new_fop_name": "new_fop_name"}
))


# ============================================================
# PHASE 2-5 TASKS (from file 1, updated URLs and enriched)
# ============================================================

def simple_body(name, desc):
    """Simple body for file(1) tasks - updated with dev webhook and _id 236."""
    return body(f'&quot;{name}&quot;', f'&quot;{desc}&quot;')

def enriched_body(name_expr, desc_expr, extra=""):
    """Body with FEEL expressions for enriched tasks."""
    return body(name_expr, desc_expr, extra) if extra else body(name_expr, desc_expr)

phase2_tasks = []

# Branch A: Landlord
phase2_tasks.append(ut_xml(
    "UT_write_sublease",
    "Написати орендодавцю лист-погодження суборенди",
    enriched_body(
        '&quot;Написати орендодавцю лист-погодження суборенди: &quot; + selected_store',
        '&quot;&lt;b&gt;Магазин:&lt;/b&gt; &quot; + selected_store + &quot;&lt;br/&gt;&lt;b&gt;Стара ФОП:&lt;/b&gt; &quot; + fop_name + &quot;&lt;br/&gt;&lt;b&gt;Нова ФОП:&lt;/b&gt; &quot; + (if is defined(new_fop_name) then new_fop_name else &quot;(не вказано)&quot;) + &quot;&lt;br/&gt;&lt;br/&gt;Написати лист орендодавцю для погодження суборенди при зміні ФОПа.&quot;'
    ),
    "Flow_parallel_to_branchA",
    "Flow_sublease_to_submit"
))

phase2_tasks.append(ut_xml(
    "UT_submit_fop_letter",
    "Подати лист на погодження ФОПа орендодавцю",
    enriched_body(
        '&quot;Подати лист на погодження ФОПа орендодавцю: &quot; + selected_store',
        '&quot;&lt;b&gt;Магазин:&lt;/b&gt; &quot; + selected_store + &quot;&lt;br/&gt;&lt;b&gt;Стара ФОП:&lt;/b&gt; &quot; + fop_name + &quot;&lt;br/&gt;&lt;b&gt;Нова ФОП:&lt;/b&gt; &quot; + (if is defined(new_fop_name) then new_fop_name else &quot;(не вказано)&quot;) + &quot;&lt;br/&gt;&lt;br/&gt;Подати підготовлений лист на погодження зміни ФОПа орендодавцю.&quot;'
    ),
    "Flow_sublease_to_submit",
    "Flow_submit_to_landlord"
))

phase2_tasks.append(ut_xml(
    "UT_landlord_check",
    "Погоджено орендодавцем?",
    simple_body("Погоджено орендодавцем?", "Перевірити чи орендодавець погодив зміну ФОПа"),
    "Flow_submit_to_landlord",
    "Flow_landlord_to_merge"
))

# Branch B: Accounting
phase2_tasks.append(ut_xml(
    "UT_check_kveds",
    "Кведи для роздрібної торгівлі є?",
    simple_body("Кведи для роздрібної торгівлі є?", "Перевірити наявність КВЕДів для роздрібної торгівлі у нового ФОПа"),
    "Flow_parallel_to_branchB",
    "Flow_kveds_to_gw"
))

phase2_tasks.append(ut_xml(
    "UT_add_kveds",
    "Внести кведи для нового ФОПа",
    simple_body("Внести кведи для нового ФОПа", "Додати необхідні КВЕДи для роздрібної торгівлі"),
    "Flow_kveds_no",
    "Flow_kveds_to_20opp"
))

phase2_tasks.append(ut_xml(
    "UT_report_20opp",
    "Подати звіт на 20 ОПП",
    simple_body("Подати звіт на 20 ОПП", "Подати звіт на форму 20 ОПП для нового ФОПа"),
    "Flow_kveds_to_20opp",
    "Flow_20opp_to_prro1"
))

phase2_tasks.append(ut_xml(
    "UT_report_prro1",
    "Подати звіт 1 ПРРО на нового ФОПа",
    simple_body("Подати звіт 1 ПРРО на нового ФОПа", "Подати звіт форма 1 ПРРО на нового ФОПа"),
    ["Flow_20opp_to_prro1", "Flow_to_yes"],
    "Flow_prro1_to_ekey"
))

phase2_tasks.append(ut_xml(
    "UT_check_ekey",
    "Електронний ключ наявний?",
    simple_body("Електронний ключ наявний?", "Перевірити наявність електронного ключа для нового ФОПа"),
    ["Flow_prro1_to_ekey", "Flow_to_no"],
    "Flow_ekey_to_gw"
))

phase2_tasks.append(ut_xml(
    "UT_create_key",
    "Створити ключ в Приват 24",
    simple_body("Створити ключ в Приват 24", "Створити новий електронний ключ через Приват 24"),
    "Flow_ekey_no",
    "Flow_key_to_prro5"
))

phase2_tasks.append(ut_xml(
    "UT_report_prro5",
    "Подаємо звіт 5 пРРО на новий ключ касира",
    simple_body("Подаємо звіт 5 пРРО на новий ключ касира", "Подати звіт форма 5 пРРО на новий ключ касира"),
    ["Flow_ekey_yes", "Flow_key_to_prro5"],
    "Flow_prro5_to_license"
))

phase2_tasks.append(ut_xml(
    "UT_check_license",
    "Ліцензія наявна?",
    simple_body("Ліцензія наявна?", "Перевірити наявність ліцензії для нового ФОПа"),
    "Flow_prro5_to_license",
    "Flow_license_to_gw"
))

phase2_tasks.append(ut_xml(
    "UT_order_license",
    "Замовити нову ліцензію",
    simple_body("Замовити нову ліцензію", "Замовити нову ліцензію для нового ФОПа"),
    "Flow_license_no",
    "Flow_license_to_webcheck"
))

phase2_tasks.append(ut_xml(
    "UT_webcheck_data",
    "Внести дані у Вебчек по новому пРРО",
    simple_body("Внести дані у Вебчек по новому пРРО", "Внести дані нового ФОПа у систему Вебчек по новому пРРО"),
    ["Flow_license_to_webcheck", "Flow_license_yes"],
    "Flow_webcheck_to_terminal"
))

phase2_tasks.append(ut_xml(
    "UT_submit_terminal",
    "Подати заявку в Приват 24 на передачу терміналу новому ФОПу",
    enriched_body(
        '&quot;Подати заявку на передачу терміналу: &quot; + (if is defined(new_fop_name) then new_fop_name else &quot;новому ФОПу&quot;)',
        '&quot;&lt;b&gt;Магазин:&lt;/b&gt; &quot; + selected_store + &quot;&lt;br/&gt;&lt;b&gt;Стара ФОП:&lt;/b&gt; &quot; + fop_name + &quot;&lt;br/&gt;&lt;b&gt;Нова ФОП:&lt;/b&gt; &quot; + (if is defined(new_fop_name) then new_fop_name else &quot;(не вказано)&quot;) + &quot;&lt;br/&gt;&lt;br/&gt;Подати заявку в Приват 24 на передачу терміналу новому ФОПу.&quot;'
    ),
    "Flow_webcheck_to_terminal",
    "Flow_terminal_to_sign"
))

phase2_tasks.append(ut_xml(
    "UT_sign_terminal",
    "Підписати заявку на прийом терміналу",
    simple_body("Підписати заявку на прийом терміналу", "Підписати заявку на прийом терміналу на новий ФОП"),
    "Flow_terminal_to_sign",
    "Flow_sign_to_par_merge"
))

# Phase 4: Date + change
phase2_tasks.append(ut_xml(
    "UT_date_reconnection",
    "Отримати дату перепідключення",
    simple_body("Отримати дату перепідключення", "Отримати та зафіксувати дату перепідключення терміналу"),
    "Flow_approved_yes",
    "Flow_date_to_gw_actual"
))

phase2_tasks.append(ut_xml(
    "UT_specify_date",
    "Вказати вірну дату",
    simple_body("Вказати вірну дату", "Вказати коректну дату перепідключення терміналу"),
    "Flow_date_actual_no",
    "Flow_specify_to_review"
))

phase2_tasks.append(ut_xml(
    "UT_review_date",
    "Ознайомитися із новою датою переключення",
    simple_body("Ознайомитися із новою датою переключення", "Ознайомитися із оновленою датою переключення терміналу"),
    "Flow_specify_to_review",
    "Flow_review_to_gw_actual"
))

phase2_tasks.append(ut_xml(
    "UT_change_fop",
    "Заміна ФОПа на терміналі",
    enriched_body(
        '&quot;Заміна ФОПа на терміналі: &quot; + selected_store',
        '&quot;&lt;b&gt;Магазин:&lt;/b&gt; &quot; + selected_store + &quot;&lt;br/&gt;&lt;b&gt;Стара ФОП:&lt;/b&gt; &quot; + fop_name + &quot;&lt;br/&gt;&lt;b&gt;Нова ФОП:&lt;/b&gt; &quot; + (if is defined(new_fop_name) then new_fop_name else &quot;(не вказано)&quot;) + &quot;&lt;br/&gt;&lt;br/&gt;Виконати заміну ФОПа на платіжному терміналі.&quot;'
    ),
    "Flow_date_arrived",
    "Flow_change_to_fill"
))

phase2_tasks.append(ut_xml(
    "UT_fill_replace",
    "Заповнити і замінити усі налаштування на новий ФОП",
    enriched_body(
        '&quot;Заповнити налаштування на новий ФОП: &quot; + (if is defined(new_fop_name) then new_fop_name else &quot;&quot;)',
        '&quot;&lt;b&gt;Магазин:&lt;/b&gt; &quot; + selected_store + &quot;&lt;br/&gt;&lt;b&gt;Нова ФОП:&lt;/b&gt; &quot; + (if is defined(new_fop_name) then new_fop_name else &quot;(не вказано)&quot;) + &quot;&lt;br/&gt;&lt;br/&gt;Заповнити і замінити усі налаштування терміналу на новий ФОП.&quot;'
    ),
    "Flow_change_to_fill",
    "Flow_fill_to_docs_parallel"
))

# Phase 5: Document finalization
phase2_tasks.append(ut_xml(
    "UT_inform_manager",
    "Інформування керівника про зміну ФОПа",
    simple_body("Інформування керівника про зміну ФОПа", "Задача про зміну ФОПа для інформування та контролю зміни документів"),
    "Flow_docs_to_inform",
    "Flow_inform_to_docs_merge"
))

phase2_tasks.append(ut_xml(
    "UT_add_documents",
    "Додати документи на нового ФОПа для Куточка споживача",
    simple_body("Додати документи на нового ФОПа для Куточка споживача", "Додати документи на нового ФОПа які необхідно надрукувати для Куточка споживача"),
    "Flow_docs_to_add_docs",
    "Flow_add_docs_to_print"
))

phase2_tasks.append(ut_xml(
    "UT_print_documents",
    "Друк документів для Куточка споживача",
    simple_body("Друк документів для Куточка споживача", "Надрукувати документи для Куточка споживача: перелік документів та дані нового ФОПа"),
    "Flow_add_docs_to_print",
    "Flow_print_to_docs_merge"
))

phase2_tasks.append(ut_xml(
    "UT_send_journals",
    "Надіслати журнали з ОП та ПБ на магазин",
    simple_body("Надіслати журнали з ОП та ПБ на магазин", "Фізично надіслати нові журнали з охорони праці та пожежної безпеки оформлені на нового ФОПа"),
    "Flow_docs_to_send_journals",
    "Flow_journals_to_process"
))

phase2_tasks.append(ut_xml(
    "UT_process_journals",
    "Оформлення журналів за інструкцією",
    simple_body("Оформлення журналів за інструкцією", "Прийняти журнали з охорони праці та пожежної безпеки та оформити відповідно до інструкцій"),
    "Flow_journals_to_process",
    "Flow_journals_to_docs_merge"
))


# ============================================================
# LANE ASSIGNMENTS
# ============================================================

lane_system = [
    "StartEvent_1", "GW_odoo_check", "ST_create_main", "GW_odoo_merge",
    "GW_is_paid", "GW_paid_merge",
    "GW_parallel_start",
    "GW_kveds_exist", "GW_to_exists", "GW_ekey_exist", "GW_license_exist",
    "GW_parallel_merge", "GW_landlord_result", "End_not_approved",
    "GW_date_actual", "GW_date_wait", "Timer_wait",
    "GW_docs_parallel_start", "GW_docs_merge", "End_final"
]

lane_bukhhalter = [
    "UT_check_stores", "UT_select_store", "UT_check_paid", "UT_notify_trc",
    "UT_pay_change", "UT_get_new_fop",
    "UT_write_sublease", "UT_submit_fop_letter",
    "UT_check_kveds", "UT_add_kveds", "UT_report_20opp", "UT_report_prro1",
    "UT_check_ekey", "UT_create_key", "UT_report_prro5",
    "UT_check_license", "UT_order_license", "UT_webcheck_data", "UT_submit_terminal",
    "UT_date_reconnection", "UT_specify_date",
    "UT_add_documents", "UT_send_journals"
]

lane_admin = [
    "UT_sign_terminal", "UT_review_date",
    "UT_change_fop", "UT_fill_replace",
    "UT_print_documents", "UT_process_journals"
]

lane_regional = ["UT_landlord_check", "UT_inform_manager"]

def lane_refs(ids):
    return "\n".join(f'        <bpmn:flowNodeRef>{i}</bpmn:flowNodeRef>' for i in ids)


# ============================================================
# BUILD COMPLETE XML
# ============================================================

xml = f'''<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI"
                  xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"
                  xmlns:di="http://www.omg.org/spec/DD/20100524/DI"
                  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
                  xmlns:modeler="http://camunda.org/schema/modeler/1.0"
                  id="Definitions_1"
                  targetNamespace="http://bpmn.io/schema/bpmn"
                  exporter="Camunda Web Modeler"
                  exporterVersion="e787c33"
                  modeler:executionPlatform="Camunda Cloud"
                  modeler:executionPlatformVersion="8.8.0">

  <bpmn:collaboration id="Collaboration_1">
    <bpmn:participant id="Participant_1" name="Сповіщення та зміна ФОП на терміналі" processRef="Process_0iy2u1a" />
  </bpmn:collaboration>

  <bpmn:process id="Process_0iy2u1a" name="Сповіщення та зміна ФОП на терміналі" isExecutable="true">
    <bpmn:extensionElements>
      <zeebe:versionTag value="4.0" />
    </bpmn:extensionElements>

    <bpmn:laneSet id="LaneSet_1">
      <bpmn:lane id="Lane_system" name="Система (автоматично)">
{lane_refs(lane_system)}
      </bpmn:lane>
      <bpmn:lane id="Lane_bukhhalter" name="Бухгалтер">
{lane_refs(lane_bukhhalter)}
      </bpmn:lane>
      <bpmn:lane id="Lane_admin_mahazyn" name="Адміністратор магазину">
{lane_refs(lane_admin)}
      </bpmn:lane>
      <bpmn:lane id="Lane_rehionalnyi" name="Регіональний керівник">
{lane_refs(lane_regional)}
      </bpmn:lane>
      <bpmn:lane id="Lane_manager" name="Керівник (ескалація)" />
    </bpmn:laneSet>

    <!-- ══════════ START EVENT ══════════ -->
    <bpmn:startEvent id="StartEvent_1" name="Перевіряємо ліміти">
      <bpmn:outgoing>Flow_start_to_check</bpmn:outgoing>
    </bpmn:startEvent>

    <!-- ══════════ XOR: Задача існує в Odoo? ══════════ -->
    <bpmn:exclusiveGateway id="GW_odoo_check" name="Задача існує в Odoo?" default="Flow_to_create">
      <bpmn:incoming>Flow_start_to_check</bpmn:incoming>
      <bpmn:outgoing>Flow_skip_create</bpmn:outgoing>
      <bpmn:outgoing>Flow_to_create</bpmn:outgoing>
    </bpmn:exclusiveGateway>

    <!-- ══════════ ST: Створити головне завдання ══════════ -->
    <bpmn:serviceTask id="ST_create_main" name="Створити головне завдання">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="http-request-smart" />
        <zeebe:ioMapping>
          <zeebe:input source="= &quot;POST&quot;" target="method" />
          <zeebe:input source="= &quot;{WEBHOOK}&quot;" target="url" />
          <zeebe:input source="= {{&quot;Content-Type&quot;:&quot;application/json&quot;}}" target="headers" />
          <zeebe:input source="= {{name: &quot;Зміна ФОП на терміналі: &quot; + fop_name, description: &quot;&lt;b&gt;ФОП:&lt;/b&gt; &quot; + fop_name + &quot; (ЄДРПОУ: &quot; + fop_edrpou + &quot;)&lt;br/&gt;&lt;b&gt;Група ЄП:&lt;/b&gt; &quot; + string(ep_group) + &quot;&lt;br/&gt;&lt;b&gt;Дохід за рік:&lt;/b&gt; &quot; + string(total_income) + &quot; грн (&quot; + string(income_percent) + &quot;% від ліміту &quot; + string(limit_amount) + &quot; грн)&lt;br/&gt;&lt;b&gt;Днів до ліміту:&lt;/b&gt; &quot; + string(days_to_limit) + &quot;&lt;br/&gt;&lt;b&gt;Прогнозна дата:&lt;/b&gt; &quot; + projected_date + &quot;&lt;br/&gt;&lt;b&gt;Магазини:&lt;/b&gt; &quot; + stores, create_process: true, _model: &quot;project.project&quot;, _id: {PROJECT_ID}, bpmn_process_id: &quot;Process_0iy2u1a&quot;, x_studio_camunda_fop_name: fop_name, x_studio_camunda_total_income: total_income, x_studio_camunda_days_to_limit: days_to_limit}}" target="body" />
        </zeebe:ioMapping>
      </bpmn:extensionElements>
      <bpmn:incoming>Flow_to_create</bpmn:incoming>
      <bpmn:outgoing>Flow_create_to_merge</bpmn:outgoing>
    </bpmn:serviceTask>

    <bpmn:exclusiveGateway id="GW_odoo_merge">
      <bpmn:incoming>Flow_create_to_merge</bpmn:incoming>
      <bpmn:incoming>Flow_skip_create</bpmn:incoming>
      <bpmn:outgoing>Flow_merge_to_check_stores</bpmn:outgoing>
    </bpmn:exclusiveGateway>

    <!-- ══════════ PHASE 1: Assessment (from file 2) ══════════ -->
{chr(10).join(phase1_tasks)}

    <!-- ══════════ XOR: Чи платна зміна? ══════════ -->
    <bpmn:exclusiveGateway id="GW_is_paid" name="Чи платна зміна?" default="Flow_not_paid">
      <bpmn:incoming>Flow_notify_to_gw_paid</bpmn:incoming>
      <bpmn:outgoing>Flow_yes_paid</bpmn:outgoing>
      <bpmn:outgoing>Flow_not_paid</bpmn:outgoing>
    </bpmn:exclusiveGateway>

    <bpmn:exclusiveGateway id="GW_paid_merge">
      <bpmn:incoming>Flow_pay_to_merge</bpmn:incoming>
      <bpmn:incoming>Flow_not_paid</bpmn:incoming>
      <bpmn:outgoing>Flow_merge_to_new_fop</bpmn:outgoing>
    </bpmn:exclusiveGateway>

    <!-- ══════════ PHASE 2: Parallel Preparation ══════════ -->
    <bpmn:parallelGateway id="GW_parallel_start">
      <bpmn:incoming>Flow_fop_to_parallel</bpmn:incoming>
      <bpmn:outgoing>Flow_parallel_to_branchA</bpmn:outgoing>
      <bpmn:outgoing>Flow_parallel_to_branchB</bpmn:outgoing>
    </bpmn:parallelGateway>

{chr(10).join(phase2_tasks)}

    <!-- Accounting branch gateways -->
    <bpmn:exclusiveGateway id="GW_kveds_exist" name="Кведи наявні?" default="Flow_kveds_no">
      <bpmn:incoming>Flow_kveds_to_gw</bpmn:incoming>
      <bpmn:outgoing>Flow_kveds_yes</bpmn:outgoing>
      <bpmn:outgoing>Flow_kveds_no</bpmn:outgoing>
    </bpmn:exclusiveGateway>

    <bpmn:exclusiveGateway id="GW_to_exists" name="Наявне ТО у підрозділу?" default="Flow_to_no">
      <bpmn:incoming>Flow_kveds_yes</bpmn:incoming>
      <bpmn:outgoing>Flow_to_yes</bpmn:outgoing>
      <bpmn:outgoing>Flow_to_no</bpmn:outgoing>
    </bpmn:exclusiveGateway>

    <bpmn:exclusiveGateway id="GW_ekey_exist" name="Наявний ел. ключ?" default="Flow_ekey_no">
      <bpmn:incoming>Flow_ekey_to_gw</bpmn:incoming>
      <bpmn:outgoing>Flow_ekey_no</bpmn:outgoing>
      <bpmn:outgoing>Flow_ekey_yes</bpmn:outgoing>
    </bpmn:exclusiveGateway>

    <bpmn:exclusiveGateway id="GW_license_exist" name="Ліцензія є?" default="Flow_license_no">
      <bpmn:incoming>Flow_license_to_gw</bpmn:incoming>
      <bpmn:outgoing>Flow_license_no</bpmn:outgoing>
      <bpmn:outgoing>Flow_license_yes</bpmn:outgoing>
    </bpmn:exclusiveGateway>

    <!-- Parallel merge -->
    <bpmn:parallelGateway id="GW_parallel_merge">
      <bpmn:incoming>Flow_landlord_to_merge</bpmn:incoming>
      <bpmn:incoming>Flow_sign_to_par_merge</bpmn:incoming>
      <bpmn:outgoing>Flow_par_merge_to_check</bpmn:outgoing>
    </bpmn:parallelGateway>

    <!-- ══════════ PHASE 3: Landlord Decision ══════════ -->
    <bpmn:exclusiveGateway id="GW_landlord_result" name="Орендодавець погодив?" default="Flow_not_approved">
      <bpmn:incoming>Flow_par_merge_to_check</bpmn:incoming>
      <bpmn:outgoing>Flow_approved_yes</bpmn:outgoing>
      <bpmn:outgoing>Flow_not_approved</bpmn:outgoing>
    </bpmn:exclusiveGateway>

    <bpmn:endEvent id="End_not_approved" name="Не погоджено орендодавцем">
      <bpmn:incoming>Flow_not_approved</bpmn:incoming>
    </bpmn:endEvent>

    <!-- ══════════ PHASE 4: Date Wait + FOP Change ══════════ -->
    <bpmn:exclusiveGateway id="GW_date_actual" name="Дата перепідключення актуальна?" default="Flow_date_actual_no">
      <bpmn:incoming>Flow_date_to_gw_actual</bpmn:incoming>
      <bpmn:incoming>Flow_review_to_gw_actual</bpmn:incoming>
      <bpmn:outgoing>Flow_date_actual_yes</bpmn:outgoing>
      <bpmn:outgoing>Flow_date_actual_no</bpmn:outgoing>
    </bpmn:exclusiveGateway>

    <bpmn:exclusiveGateway id="GW_date_wait" name="Дата настала?" default="Flow_date_not_arrived">
      <bpmn:incoming>Flow_date_actual_yes</bpmn:incoming>
      <bpmn:incoming>Flow_timer_to_gw_wait</bpmn:incoming>
      <bpmn:outgoing>Flow_date_not_arrived</bpmn:outgoing>
      <bpmn:outgoing>Flow_date_arrived</bpmn:outgoing>
    </bpmn:exclusiveGateway>

    <bpmn:intermediateCatchEvent id="Timer_wait" name="Чекаємо дати перепідключення">
      <bpmn:incoming>Flow_date_not_arrived</bpmn:incoming>
      <bpmn:outgoing>Flow_timer_to_gw_wait</bpmn:outgoing>
      <bpmn:timerEventDefinition id="TD_timer_wait">
        <bpmn:timeDuration xsi:type="bpmn:tFormalExpression">PT24H</bpmn:timeDuration>
      </bpmn:timerEventDefinition>
    </bpmn:intermediateCatchEvent>

    <!-- ══════════ PHASE 5: Document Finalization ══════════ -->
    <bpmn:parallelGateway id="GW_docs_parallel_start">
      <bpmn:incoming>Flow_fill_to_docs_parallel</bpmn:incoming>
      <bpmn:outgoing>Flow_docs_to_inform</bpmn:outgoing>
      <bpmn:outgoing>Flow_docs_to_add_docs</bpmn:outgoing>
      <bpmn:outgoing>Flow_docs_to_send_journals</bpmn:outgoing>
    </bpmn:parallelGateway>

    <bpmn:parallelGateway id="GW_docs_merge">
      <bpmn:incoming>Flow_inform_to_docs_merge</bpmn:incoming>
      <bpmn:incoming>Flow_print_to_docs_merge</bpmn:incoming>
      <bpmn:incoming>Flow_journals_to_docs_merge</bpmn:incoming>
      <bpmn:outgoing>Flow_docs_merge_to_end</bpmn:outgoing>
    </bpmn:parallelGateway>

    <bpmn:endEvent id="End_final" name="Процес завершено">
      <bpmn:incoming>Flow_docs_merge_to_end</bpmn:incoming>
    </bpmn:endEvent>

    <!-- ══════════ SEQUENCE FLOWS ══════════ -->
    <!-- Start + Odoo check -->
    <bpmn:sequenceFlow id="Flow_start_to_check" sourceRef="StartEvent_1" targetRef="GW_odoo_check" />
    <bpmn:sequenceFlow id="Flow_skip_create" sourceRef="GW_odoo_check" targetRef="UT_check_stores">
      <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression">= is defined(odoo_task_id) and odoo_task_id != null</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="Flow_to_create" sourceRef="GW_odoo_check" targetRef="ST_create_main" />
    <bpmn:sequenceFlow id="Flow_create_to_merge" sourceRef="ST_create_main" targetRef="UT_check_stores" />

    <!-- Phase 1 flows -->
    <bpmn:sequenceFlow id="Flow_stores_to_select" sourceRef="UT_check_stores" targetRef="UT_select_store" />
    <bpmn:sequenceFlow id="Flow_select_to_paid" sourceRef="UT_select_store" targetRef="UT_check_paid" />
    <bpmn:sequenceFlow id="Flow_paid_to_notify" sourceRef="UT_check_paid" targetRef="UT_notify_trc" />
    <bpmn:sequenceFlow id="Flow_notify_to_gw_paid" sourceRef="UT_notify_trc" targetRef="GW_is_paid" />
    <bpmn:sequenceFlow id="Flow_yes_paid" name="так" sourceRef="GW_is_paid" targetRef="UT_pay_change">
      <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression">= is_paid_change = true</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="Flow_not_paid" sourceRef="GW_is_paid" targetRef="UT_get_new_fop" />
    <bpmn:sequenceFlow id="Flow_pay_to_merge" sourceRef="UT_pay_change" targetRef="UT_get_new_fop" />
    <bpmn:sequenceFlow id="Flow_fop_to_parallel" sourceRef="UT_get_new_fop" targetRef="GW_parallel_start" />

    <!-- Parallel split -->
    <bpmn:sequenceFlow id="Flow_parallel_to_branchA" sourceRef="GW_parallel_start" targetRef="UT_write_sublease" />
    <bpmn:sequenceFlow id="Flow_parallel_to_branchB" sourceRef="GW_parallel_start" targetRef="UT_check_kveds" />

    <!-- Branch A: Landlord -->
    <bpmn:sequenceFlow id="Flow_sublease_to_submit" sourceRef="UT_write_sublease" targetRef="UT_submit_fop_letter" />
    <bpmn:sequenceFlow id="Flow_submit_to_landlord" sourceRef="UT_submit_fop_letter" targetRef="UT_landlord_check" />
    <bpmn:sequenceFlow id="Flow_landlord_to_merge" sourceRef="UT_landlord_check" targetRef="GW_parallel_merge" />

    <!-- Branch B: Accounting -->
    <bpmn:sequenceFlow id="Flow_kveds_to_gw" sourceRef="UT_check_kveds" targetRef="GW_kveds_exist" />
    <bpmn:sequenceFlow id="Flow_kveds_yes" sourceRef="GW_kveds_exist" targetRef="GW_to_exists">
      <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression">= x_studio_camunda_kveds_exist = true</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="Flow_kveds_no" sourceRef="GW_kveds_exist" targetRef="UT_add_kveds" />
    <bpmn:sequenceFlow id="Flow_kveds_to_20opp" sourceRef="UT_add_kveds" targetRef="UT_report_20opp" />
    <bpmn:sequenceFlow id="Flow_to_yes" sourceRef="GW_to_exists" targetRef="UT_report_prro1">
      <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression">= x_studio_camunda_to_exists = true</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="Flow_to_no" sourceRef="GW_to_exists" targetRef="UT_check_ekey" />
    <bpmn:sequenceFlow id="Flow_20opp_to_prro1" sourceRef="UT_report_20opp" targetRef="UT_report_prro1" />
    <bpmn:sequenceFlow id="Flow_prro1_to_ekey" sourceRef="UT_report_prro1" targetRef="UT_check_ekey" />
    <bpmn:sequenceFlow id="Flow_ekey_to_gw" sourceRef="UT_check_ekey" targetRef="GW_ekey_exist" />
    <bpmn:sequenceFlow id="Flow_ekey_yes" sourceRef="GW_ekey_exist" targetRef="UT_report_prro5">
      <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression">= x_studio_camunda_ekey_exist = true</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="Flow_ekey_no" sourceRef="GW_ekey_exist" targetRef="UT_create_key" />
    <bpmn:sequenceFlow id="Flow_key_to_prro5" sourceRef="UT_create_key" targetRef="UT_report_prro5" />
    <bpmn:sequenceFlow id="Flow_prro5_to_license" sourceRef="UT_report_prro5" targetRef="UT_check_license" />
    <bpmn:sequenceFlow id="Flow_license_to_gw" sourceRef="UT_check_license" targetRef="GW_license_exist" />
    <bpmn:sequenceFlow id="Flow_license_no" sourceRef="GW_license_exist" targetRef="UT_order_license" />
    <bpmn:sequenceFlow id="Flow_license_yes" sourceRef="GW_license_exist" targetRef="UT_webcheck_data">
      <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression">= x_studio_camunda_license_exist = true</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="Flow_license_to_webcheck" sourceRef="UT_order_license" targetRef="UT_webcheck_data" />
    <bpmn:sequenceFlow id="Flow_webcheck_to_terminal" sourceRef="UT_webcheck_data" targetRef="UT_submit_terminal" />
    <bpmn:sequenceFlow id="Flow_terminal_to_sign" sourceRef="UT_submit_terminal" targetRef="UT_sign_terminal" />
    <bpmn:sequenceFlow id="Flow_sign_to_par_merge" sourceRef="UT_sign_terminal" targetRef="GW_parallel_merge" />

    <!-- Phase 3: Landlord decision -->
    <bpmn:sequenceFlow id="Flow_par_merge_to_check" sourceRef="GW_parallel_merge" targetRef="GW_landlord_result" />
    <bpmn:sequenceFlow id="Flow_approved_yes" name="Так" sourceRef="GW_landlord_result" targetRef="UT_date_reconnection">
      <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression">= x_studio_camunda_landlord_approved = true</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="Flow_not_approved" sourceRef="GW_landlord_result" targetRef="End_not_approved" />

    <!-- Phase 4: Date loop -->
    <bpmn:sequenceFlow id="Flow_date_to_gw_actual" sourceRef="UT_date_reconnection" targetRef="GW_date_actual" />
    <bpmn:sequenceFlow id="Flow_date_actual_yes" sourceRef="GW_date_actual" targetRef="GW_date_wait">
      <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression">= x_studio_camunda_date_actual = true</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="Flow_date_actual_no" sourceRef="GW_date_actual" targetRef="UT_specify_date" />
    <bpmn:sequenceFlow id="Flow_specify_to_review" sourceRef="UT_specify_date" targetRef="UT_review_date" />
    <bpmn:sequenceFlow id="Flow_review_to_gw_actual" sourceRef="UT_review_date" targetRef="GW_date_actual" />
    <bpmn:sequenceFlow id="Flow_date_not_arrived" sourceRef="GW_date_wait" targetRef="Timer_wait" />
    <bpmn:sequenceFlow id="Flow_timer_to_gw_wait" sourceRef="Timer_wait" targetRef="GW_date_wait" />
    <bpmn:sequenceFlow id="Flow_date_arrived" sourceRef="GW_date_wait" targetRef="UT_change_fop">
      <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression">= x_studio_camunda_date_arrived = true</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="Flow_change_to_fill" sourceRef="UT_change_fop" targetRef="UT_fill_replace" />

    <!-- Phase 5: Document finalization -->
    <bpmn:sequenceFlow id="Flow_fill_to_docs_parallel" sourceRef="UT_fill_replace" targetRef="GW_docs_parallel_start" />
    <bpmn:sequenceFlow id="Flow_docs_to_inform" sourceRef="GW_docs_parallel_start" targetRef="UT_inform_manager" />
    <bpmn:sequenceFlow id="Flow_docs_to_add_docs" sourceRef="GW_docs_parallel_start" targetRef="UT_add_documents" />
    <bpmn:sequenceFlow id="Flow_docs_to_send_journals" sourceRef="GW_docs_parallel_start" targetRef="UT_send_journals" />
    <bpmn:sequenceFlow id="Flow_add_docs_to_print" sourceRef="UT_add_documents" targetRef="UT_print_documents" />
    <bpmn:sequenceFlow id="Flow_journals_to_process" sourceRef="UT_send_journals" targetRef="UT_process_journals" />
    <bpmn:sequenceFlow id="Flow_inform_to_docs_merge" sourceRef="UT_inform_manager" targetRef="GW_docs_merge" />
    <bpmn:sequenceFlow id="Flow_print_to_docs_merge" sourceRef="UT_print_documents" targetRef="GW_docs_merge" />
    <bpmn:sequenceFlow id="Flow_journals_to_docs_merge" sourceRef="UT_process_journals" targetRef="GW_docs_merge" />
    <bpmn:sequenceFlow id="Flow_docs_merge_to_end" sourceRef="GW_docs_merge" targetRef="End_final" />

  </bpmn:process>

  <!-- ══════════ DI (placeholder — run bpmn_auto_layout.py to fix) ══════════ -->
  <bpmndi:BPMNDiagram id="BPMNDiagram_1">
    <bpmndi:BPMNPlane id="BPMNPlane_1" bpmnElement="Collaboration_1">
      <bpmndi:BPMNShape id="Participant_1_di" bpmnElement="Participant_1" isHorizontal="true">
        <dc:Bounds x="105" y="90" width="6200" height="1350" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Lane_system_di" bpmnElement="Lane_system" isHorizontal="true">
        <dc:Bounds x="135" y="90" width="6170" height="200" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Lane_bukhhalter_di" bpmnElement="Lane_bukhhalter" isHorizontal="true">
        <dc:Bounds x="135" y="290" width="6170" height="400" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Lane_admin_mahazyn_di" bpmnElement="Lane_admin_mahazyn" isHorizontal="true">
        <dc:Bounds x="135" y="690" width="6170" height="250" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Lane_rehionalnyi_di" bpmnElement="Lane_rehionalnyi" isHorizontal="true">
        <dc:Bounds x="135" y="940" width="6170" height="250" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Lane_manager_di" bpmnElement="Lane_manager" isHorizontal="true">
        <dc:Bounds x="135" y="1190" width="6170" height="250" />
      </bpmndi:BPMNShape>
'''

# Generate DI shapes with simple coordinates
# Lane_system center y=190, Lane_bukhhalter center y=490, Lane_admin center y=815, Lane_regional center y=1065

shapes = {
    # System lane elements (y=165 for gateways, y=172 for events, y=150 for tasks)
    "StartEvent_1": (200, 172, 36, 36),
    "GW_odoo_check": (300, 165, 50, 50),
    "ST_create_main": (400, 150, 100, 80),
    "GW_odoo_merge": (560, 165, 50, 50),
    # Phase 1 in bukhhalter (y=450 for tasks)
    "UT_check_stores": (660, 450, 100, 80),
    "UT_select_store": (840, 450, 100, 80),
    "UT_check_paid": (1020, 450, 100, 80),
    "UT_notify_trc": (1200, 450, 100, 80),
    "GW_is_paid": (1360, 165, 50, 50),
    "UT_pay_change": (1460, 560, 100, 80),
    "GW_paid_merge": (1610, 165, 50, 50),
    "UT_get_new_fop": (1710, 450, 100, 80),
    # Phase 2 start
    "GW_parallel_start": (1870, 165, 50, 50),
    # Branch A (bukhhalter + admin + regional)
    "UT_write_sublease": (1970, 450, 100, 80),
    "UT_submit_fop_letter": (2150, 450, 100, 80),
    "UT_landlord_check": (2330, 1025, 100, 80),
    # Branch B (bukhhalter)
    "UT_check_kveds": (1970, 350, 100, 80),
    "GW_kveds_exist": (2120, 365, 50, 50),
    "UT_add_kveds": (2220, 450, 100, 80),
    "GW_to_exists": (2220, 315, 50, 50),
    "UT_report_20opp": (2400, 450, 100, 80),
    "UT_report_prro1": (2400, 350, 100, 80),
    "UT_check_ekey": (2580, 350, 100, 80),
    "GW_ekey_exist": (2730, 365, 50, 50),
    "UT_create_key": (2830, 450, 100, 80),
    "UT_report_prro5": (3010, 350, 100, 80),
    "UT_check_license": (3190, 350, 100, 80),
    "GW_license_exist": (3340, 365, 50, 50),
    "UT_order_license": (3440, 450, 100, 80),
    "UT_webcheck_data": (3620, 350, 100, 80),
    "UT_submit_terminal": (3800, 350, 100, 80),
    "UT_sign_terminal": (3980, 775, 100, 80),
    # Phase 3
    "GW_parallel_merge": (4160, 165, 50, 50),
    "GW_landlord_result": (4310, 165, 50, 50),
    "End_not_approved": (4350, 250, 36, 36),
    # Phase 4
    "UT_date_reconnection": (4460, 450, 100, 80),
    "GW_date_actual": (4620, 165, 50, 50),
    "UT_specify_date": (4720, 450, 100, 80),
    "UT_review_date": (4720, 775, 100, 80),
    "GW_date_wait": (4870, 165, 50, 50),
    "Timer_wait": (4870, 250, 36, 36),
    "UT_change_fop": (5020, 775, 100, 80),
    "UT_fill_replace": (5200, 775, 100, 80),
    # Phase 5
    "GW_docs_parallel_start": (5380, 165, 50, 50),
    "UT_inform_manager": (5480, 1025, 100, 80),
    "UT_add_documents": (5480, 450, 100, 80),
    "UT_send_journals": (5480, 560, 100, 80),
    "UT_print_documents": (5660, 775, 100, 80),
    "UT_process_journals": (5660, 900, 100, 80),
    "GW_docs_merge": (5840, 165, 50, 50),
    "End_final": (5960, 172, 36, 36),
}

for eid, (x, y, w, h) in shapes.items():
    is_gw = eid.startswith("GW_")
    marker = ' isMarkerVisible="true"' if is_gw else ''
    xml += f'      <bpmndi:BPMNShape id="{eid}_di" bpmnElement="{eid}"{marker}>\n'
    xml += f'        <dc:Bounds x="{x}" y="{y}" width="{w}" height="{h}" />\n'
    xml += f'      </bpmndi:BPMNShape>\n'

# Simple edge DI — just 2 waypoints (source right center → target left center)
edges = [
    "Flow_start_to_check", "Flow_skip_create", "Flow_to_create", "Flow_create_to_merge",
    "Flow_stores_to_select", "Flow_select_to_paid", "Flow_paid_to_notify",
    "Flow_notify_to_gw_paid", "Flow_yes_paid", "Flow_not_paid", "Flow_pay_to_merge",
    "Flow_fop_to_parallel",
    "Flow_parallel_to_branchA", "Flow_parallel_to_branchB",
    "Flow_sublease_to_submit", "Flow_submit_to_landlord", "Flow_landlord_to_merge",
    "Flow_kveds_to_gw", "Flow_kveds_yes", "Flow_kveds_no", "Flow_kveds_to_20opp",
    "Flow_to_yes", "Flow_to_no", "Flow_20opp_to_prro1", "Flow_prro1_to_ekey",
    "Flow_ekey_to_gw", "Flow_ekey_yes", "Flow_ekey_no", "Flow_key_to_prro5",
    "Flow_prro5_to_license", "Flow_license_to_gw", "Flow_license_no", "Flow_license_yes",
    "Flow_license_to_webcheck", "Flow_webcheck_to_terminal", "Flow_terminal_to_sign",
    "Flow_sign_to_par_merge",
    "Flow_par_merge_to_check", "Flow_approved_yes", "Flow_not_approved",
    "Flow_date_to_gw_actual", "Flow_date_actual_yes", "Flow_date_actual_no",
    "Flow_specify_to_review", "Flow_review_to_gw_actual",
    "Flow_date_not_arrived", "Flow_timer_to_gw_wait", "Flow_date_arrived",
    "Flow_change_to_fill", "Flow_fill_to_docs_parallel",
    "Flow_docs_to_inform", "Flow_docs_to_add_docs", "Flow_docs_to_send_journals",
    "Flow_add_docs_to_print", "Flow_journals_to_process",
    "Flow_inform_to_docs_merge", "Flow_print_to_docs_merge", "Flow_journals_to_docs_merge",
    "Flow_docs_merge_to_end",
    "Flow_merge_to_check_stores",
    "Flow_merge_to_new_fop",
]

# Simple placeholder edges: just horizontal at y=190
for eid in edges:
    xml += f'      <bpmndi:BPMNEdge id="{eid}_di" bpmnElement="{eid}">\n'
    xml += f'        <di:waypoint x="0" y="190" />\n'
    xml += f'        <di:waypoint x="100" y="190" />\n'
    xml += f'      </bpmndi:BPMNEdge>\n'

xml += '''    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>

</bpmn:definitions>
'''

# Fix: remove GW_odoo_merge since we route directly to UT_check_stores
# The skip_create and create_to_merge flows go directly to UT_check_stores
# So we don't need GW_odoo_merge at all — both paths merge at UT_check_stores

# Actually we already handle this: UT_check_stores has incoming [Flow_skip_create, Flow_create_to_merge]
# But we still have GW_odoo_merge defined with no connections. Let's remove it.
xml = xml.replace('''    <bpmn:exclusiveGateway id="GW_odoo_merge">
      <bpmn:incoming>Flow_create_to_merge</bpmn:incoming>
      <bpmn:incoming>Flow_skip_create</bpmn:incoming>
      <bpmn:outgoing>Flow_merge_to_check_stores</bpmn:outgoing>
    </bpmn:exclusiveGateway>
''', '')

# Remove GW_odoo_merge from lane_system refs
xml = xml.replace('        <bpmn:flowNodeRef>GW_odoo_merge</bpmn:flowNodeRef>\n', '')

# Remove Flow_merge_to_check_stores (not needed)
xml = xml.replace('    <bpmn:sequenceFlow id="Flow_merge_to_check_stores" sourceRef="GW_odoo_merge" targetRef="UT_check_stores" />\n', '')

# Also remove GW_paid_merge — both paths merge at UT_get_new_fop directly
xml = xml.replace('''    <bpmn:exclusiveGateway id="GW_paid_merge">
      <bpmn:incoming>Flow_pay_to_merge</bpmn:incoming>
      <bpmn:incoming>Flow_not_paid</bpmn:incoming>
      <bpmn:outgoing>Flow_merge_to_new_fop</bpmn:outgoing>
    </bpmn:exclusiveGateway>
''', '')
xml = xml.replace('        <bpmn:flowNodeRef>GW_paid_merge</bpmn:flowNodeRef>\n', '')
xml = xml.replace('    <bpmn:sequenceFlow id="Flow_merge_to_new_fop" sourceRef="GW_paid_merge" targetRef="UT_get_new_fop" />\n', '')

# Write the file
output_path = "bpmn/zmina-fopa-na-terminali/Сповіщення про зміну ФОП (2).bpmn"
with open(output_path, "w", encoding="utf-8") as f:
    f.write(xml)

print(f"Written merged BPMN to: {output_path}")
print(f"Total lines: {xml.count(chr(10))}")
