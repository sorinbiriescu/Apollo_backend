import altair as alt
import pandas as pd


def create_tickets_age_chart(df):
    df_altair = df.loc[:,("ticket_number", "configuration_item", "age")]
    df_altair["age"] = df_altair["age"] / pd.Timedelta(days = 1)

    interval = alt.selection_interval(encodings=['x'])
    single = alt.selection_interval(encodings=['x'])

    ci_count = alt.Chart(df_altair).mark_bar().encode(
        x = alt.X(
            'configuration_item:N',
            title='CI',
            sort=alt.EncodingSortField(field="configuration_item", op="count", order='descending')),
        y = alt.Y(
            'count()',
            title='Total'
            ),
        color = alt.condition(single, 'count()', alt.value('grey'), legend = alt.Legend(title="Total"))
    ).properties(
        width = 600,
        height = 200
    ).add_selection(
        single
    ).transform_filter(
        interval
    )


    time_duration = alt.Chart(df_altair).mark_bar().encode(
        x = alt.X(
            'age:Q',
            bin=alt.Bin(maxbins = 50),
            title='Age (jours)'),
        y = alt.Y(
            'count()',
            title='Total')
    ).properties(
        selection = interval,
        width = 600,
        height = 100
    )

    ranked_text = alt.Chart(df_altair).mark_text().encode(
        y = alt.Y(
            'row_number:O',axis=None
            )
    ).transform_window(
        row_number='row_number()'
    ).transform_filter(
        {'and': [single, interval]}
    ).transform_window(
        rank='rank(row_number:O)'
    ).transform_filter(
        alt.datum.rank<25
    )

    ticket_number = ranked_text.encode(text='ticket_number:N').properties(title='# Dossier')
    configuraton_item = ranked_text.encode(text='configuration_item:N').properties(title='CI', width = 200)
    text = alt.hconcat(ticket_number, configuraton_item)

    final_chart = (ci_count & time_duration) | text
    return final_chart.to_json()


def create_tickets_points_chart(df):
    df_altair = df.loc[:,("ticket_number", "configuration_item", "points")]

    interval = alt.selection_interval(encodings=['x'])
    single = alt.selection_interval(encodings=['x'])

    ci_count = alt.Chart(df_altair).mark_bar().encode(
        x = alt.X(
            'configuration_item:N',
            title='CI',
            sort=alt.EncodingSortField(field="configuration_item", op="count", order='descending')),
        y = alt.Y(
            'count()',
            title='Total'
            ),
        color = alt.condition(single, 'count()', alt.value('grey'), legend = alt.Legend(title="Total"))
    ).properties(
        width = 600,
        height = 200
    ).add_selection(
        single
    ).transform_filter(
        interval
    )


    time_duration = alt.Chart(df_altair).mark_bar().encode(
        x = alt.X(
            'points:Q',
            bin=alt.Bin(maxbins = 50),
            title='Points'),
        y = alt.Y(
            'count()',
            title='Total')
    ).properties(
        selection = interval,
        width = 600,
        height = 100
    )

    ranked_text = alt.Chart(df_altair).mark_text().encode(
        y = alt.Y(
            'row_number:O',axis=None
            )
    ).transform_window(
        row_number='row_number()'
    ).transform_filter(
        {'and': [single, interval]}
    ).transform_window(
        rank='rank(row_number:O)'
    ).transform_filter(
        alt.datum.rank<25
    )

    ticket_number = ranked_text.encode(text='ticket_number:N').properties(title='# Dossier')
    configuraton_item = ranked_text.encode(text='configuration_item:N').properties(title='CI', width = 200)
    text = alt.hconcat(ticket_number, configuraton_item)

    final_chart = (ci_count & time_duration) | text
    return final_chart.to_json()


def create_tickets_by_inter_type_age_chart(df):
    df_altair = df.loc[:,("ticket_number", "inter_type", "configuration_item", "age")]
    df_altair["age"] = df_altair["age"] / pd.Timedelta(days = 1)

    interval = alt.selection_interval(encodings=['x'])
    single = alt.selection_interval(encodings=['x'])

    inter_type_count = alt.Chart(df_altair).mark_bar().encode(
        x = alt.X(
            'inter_type:N',
            title='CI'
        ),
        y = alt.Y(
            'count()',
            title='Total'
            ),
        color = alt.condition(single, 'count()', alt.value('grey'), legend = alt.Legend(title="Total"))
    ).properties(
        width = 600,
        height = 200
    ).add_selection(
        single
    ).transform_filter(
        interval
    )

    time_duration = alt.Chart(df_altair).mark_bar().encode(
        x = alt.X(
            'age:Q',
            bin=alt.Bin(maxbins = 50),
            title='Age (jours)'),
        y = alt.Y(
            'count()',
            title='Total')
    ).properties(
        selection = interval,
        width = 600,
        height = 100
    )

    ranked_text = alt.Chart(df_altair).mark_text().encode(
        y = alt.Y(
            'row_number:O',axis=None
            )
    ).transform_window(
        row_number='row_number()'
    ).transform_filter(
        {'and': [single, interval]}
    ).transform_window(
        rank='rank(row_number:O)'
    ).transform_filter(
        alt.datum.rank<25
    )

    ticket_number = ranked_text.encode(text='ticket_number:N').properties(title='# Dossier')
    configuraton_item = ranked_text.encode(text='configuration_item:N').properties(title='CI', width = 200)
    text = alt.hconcat(ticket_number, configuraton_item)

    final_chart = (inter_type_count & time_duration) | text
    return final_chart.to_json()


def create_tickets_by_inter_type_points_chart(df):
    df_altair = df.loc[:,("ticket_number", "inter_type", "configuration_item", "points")]

    interval = alt.selection_interval(encodings=['x'])
    single = alt.selection_interval(encodings=['x'])

    inter_type_count = alt.Chart(df_altair).mark_bar().encode(
        x = alt.X(
            'inter_type:N',
            title='CI'
        ),
        y = alt.Y(
            'count()',
            title='Total'
            ),
        color = alt.condition(single, 'count()', alt.value('grey'), legend = alt.Legend(title="Total"))
    ).properties(
        width = 600,
        height = 200
    ).add_selection(
        single
    ).transform_filter(
        interval
    )

    time_duration = alt.Chart(df_altair).mark_bar().encode(
        x = alt.X(
            'points:Q',
            bin=alt.Bin(maxbins = 50),
            title='Points (total)'),
        y = alt.Y(
            'count()',
            title='Total')
    ).properties(
        selection = interval,
        width = 600,
        height = 100
    )

    ranked_text = alt.Chart(df_altair).mark_text().encode(
        y = alt.Y(
            'row_number:O',axis=None
            )
    ).transform_window(
        row_number='row_number()'
    ).transform_filter(
        {'and': [single, interval]}
    ).transform_window(
        rank='rank(row_number:O)'
    ).transform_filter(
        alt.datum.rank<25
    )

    ticket_number = ranked_text.encode(text='ticket_number:N').properties(title='# Dossier')
    configuraton_item = ranked_text.encode(text='configuration_item:N').properties(title='CI', width = 200)
    text = alt.hconcat(ticket_number, configuraton_item)

    final_chart = (inter_type_count & time_duration) | text
    return final_chart.to_json()
