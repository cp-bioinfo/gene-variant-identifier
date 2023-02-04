import pandas as pd


def reset_categorical_index(df):
    # just a straight reset_index does not work with
    # CategoricalIndexes so we have to do it ourselves
    # credit: https://github.com/pandas-dev/pandas/issues/19136#issuecomment-380908428
    return pd.merge(
        df.index.to_frame(index=False),
        df.reset_index(drop=True),
        left_index=True,
        right_index=True
    )


def df_append(df1, df2, merge_categories=False, **kwargs):
    """ Append retaining category dtypes """
    if isinstance(df2, type(None)):
        return df1

    if merge_categories:
        for col in df1.columns:
            c1 = df1[col]
            if c1.dtype.name == 'category':
                if col in df2:
                    c2 = df2[col]
                    if c2.dtype.name != 'category' or set(c1.cat.categories) == set(c2.cat.categories):
                        continue
                    new_cats = df1[col].cat.categories | df2[col].cat.categories
                    c1.cat.set_categories(new_cats, inplace=True)
                    c2.cat.set_categories(new_cats, inplace=True)

    return df1.append(df2, **kwargs)
