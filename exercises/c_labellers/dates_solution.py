import datetime

import holidays
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as sf
from pyspark.sql.types import BooleanType, DateType, StructField, StructType

MIN_YEAR_FOR_HOLIDAYS = 1950
MAX_YEAR_FOR_HOLIDAYS = 2100


def is_belgian_holiday(date: datetime.date) -> bool:
    belgian_holidays = holidays.BE()
    return date in belgian_holidays


def label_weekend(
    frame: DataFrame, colname: str = "date", new_colname: str = "is_weekend"
) -> DataFrame:
    """Add a column with the name of `new_colname`, indicating whether the
    column bearing the name `colname` is a weekend day."""
    # The line below is one solution. There is a more performant version of it
    # too, involving the modulo operator, but it is more complex. The
    # performance gain might not outweigh the cost of a programmer trying to
    # understand that arithmetic.

    # Keep in mind, that we're using "magic numbers" here too: unless you know
    # the API by heart, you're better off defining "SUNDAY = 1" and
    # "SATURDAY = 7" and using those identifiers in the call to the `isin`
    # method.

    # Finally, programming languages and even frameworks within the same
    # programming language tend to differ in the convention whether Monday is
    # day 1 or not. You should always check the documentation corresponding to
    # your library.
    return frame.withColumn(new_colname, sf.dayofweek(colname)).isin(1, 7)


def label_holidays(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Add a column indicating whether the column `colname` is a holiday."""

    # holiday_udf = udf(lambda z: is_belgian_holiday(z), BooleanType())

    # If you were to see something like the line above in serious code, the
    # author of that line might not have understood the concepts of lambda
    # functions (nameless functions) and function references well. The
    # assignment above is more efficiently written as:
    holiday_udf = sf.udf(is_belgian_holiday, BooleanType())

    return frame.withColumn(new_colname, holiday_udf(colname))


def label_holidays2(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Add a column indicating whether the column `colname` is a holiday."""

    # A more efficient implementation of `label_holidays` than the udf-variant.
    # Major downside is that the range of years needs to be known a priori. Put
    # them in a config file or extract the range from the data beforehand.
    holidays_be = holidays.BE(
        years=list(range(MIN_YEAR_FOR_HOLIDAYS, MAX_YEAR_FOR_HOLIDAYS))
    )
    max_date, min_date = frame.agg(
        max(colname).alias("max_date"), min(colname).alias("min_date")
    ).first()
    my_holidays = holidays.BE(years=list(range(min_date.year, max_date.year)))
    return frame.withColumn(
        new_colname, frame[colname].isin(my_holidays.keys())
    )


def label_holidays3(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Add a column indicating whether the column `colname` is a holiday."""

    # Another more efficient implementation of `label_holidays`. Same downsides
    # as label_holidays2, but scales better.
    holidays_be = holidays.BE(
        years=list(range(MIN_YEAR_FOR_HOLIDAYS, MAX_YEAR_FOR_HOLIDAYS))
    )

    # Since you're expecting a pyspark.sql.DataFrame in this function, you
    # *know* there's an existing SparkSession. You can get it like this:
    spark = SparkSession.getActiveSession()
    # alternatively, like this:
    # spark = frame.sql_ctx.sparkSession
    # or in more recent versions, simply frame.sparkSession
    # Both return the same session. The latter is guaranteed to return a
    # SparkSession, the former only does it best effort and won't complain if
    # there's no active SparkSession. Static type checking tools, like mypy,
    # will raise warnings on that.
    holidays_frame = spark.createDataFrame(
        data=[(day, True) for day in holidays_be.keys()],
        schema=StructType(
            [
                StructField(colname, DateType(), False),
                StructField(new_colname, BooleanType(), False),
            ]
        ),
    )
    holidays_frame.show(51)

    part1 = frame.join(holidays_frame, on=colname, how="left")

    part1.show()

    part2 = part1.na.fill(False, subset=[new_colname])
    part2.show()
    # If the input date column can contain nulls, then the correct answer would be this:
    part2 = part1.withColumn(
        new_colname,
        sf.when(frame[colname].isNull(), None).otherwise(
            sf.coalesce(new_colname, sf.lit(False))
        ),
    )
    # That's because you want to preserve the nulls from the input column (if
    # you don't know the date, you can't say whether it's a holiday either, so
    # null -> null).
    return part2
