import pandas as pd
import json
import numpy as np

class OutputWriter:

    def __init__(self, db_obj):
        self.db_obj = db_obj
        self.header_colors = ['#c1f5a9', '#34e5eb', '#ebd496', '#c9c9f5', '#e8caed']
        self.historical_keyword_data()

    def highlight_mapped_skus(self,val):
        print (val)

    def historical_keyword_data(self):
        i=-1
        try:
            data = self.db_obj.get_historical_keyword_data()
            try:
                with open('Results/JSON/output.json','w') as d:
                    json.dump(data,d)
            except Exception as ee:
                print ("Exception Output Writer Json write:- ",ee)

            for keyword_data_obj in data:
                i += 1
                for keyword in keyword_data_obj:
                    keyword_data_obj = keyword_data_obj.get(keyword, [])
                    file_name = 'Results/{} _ results.xlsx'.format(keyword)
                    with pd.ExcelWriter(file_name,
                                        engine='xlsxwriter') as writer:
                        workbook = writer.book

                        all_format = workbook.add_format({
                            'valign': 'vcenter',
                            'align': 'center'})
                        self_sku_format = workbook.add_format({'valign': 'vcenter',
                                                               'align': 'center',
                                                               'font_color': '#b8e874'})
                        comp_sku_format = workbook.add_format({'valign': 'vcenter',
                                                               'align': 'center',
                                                               'font_color': '#eb6154'})

                        for date_ in keyword_data_obj:
                            date_obj = keyword_data_obj.get(date_, [])
                            df = pd.read_json(json.dumps(date_obj))
                            self_rows = np.where(df['SKU Type'].isin(['Self']),
                                                 'background-color: #94d964',
                                                 '')
                            comp_rows = np.where(df['SKU Type'].isin(['Competitor']),
                                                 'background-color: #ed645c',
                                                 '')
                            # Apply calculated styles to each column:
                            styler = df.style.apply(lambda _: self_rows)
                            # styler = df.style.apply(lambda _: comp_rows)
                            styler.to_excel(writer, sheet_name=date_, index=False, startrow=1, header=False)

                            worksheet = writer.sheets[date_]
                            worksheet.set_column('A:K', None, all_format)

                            header_color = self.header_colors[i]
                            header_format = workbook.add_format({
                                'bold': True,
                                'text_wrap': True,
                                'valign': 'vcenter',
                                'align': 'center',
                                'bg_color': header_color,
                                'border': 1})
                            for col_num, value in enumerate(df.columns.values):
                                worksheet.write(0, col_num, value, header_format)
                            worksheet.set_column('A:A', 10)
                            worksheet.set_column('B:B', 20)
                            worksheet.set_column('C:D', 30)
                            worksheet.set_column('E:E', 20)
                            worksheet.set_column('F:F', 30)
                            worksheet.set_column('G:K', 20)
        except Exception as e:
            print ("Exception in output writer :-",e)
