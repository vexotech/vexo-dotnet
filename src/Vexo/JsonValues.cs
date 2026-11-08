using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Vexo;

/// <summary>
/// Converters that write property and trait values in the form every Vexo SDK sends them (SPEC.md M11): a date-time in
/// UTC with milliseconds, <c>2026-11-07T12:00:00.000Z</c>, and a <see cref="decimal"/> as a string, so that a reader of
/// the JSON keeps every digit.
/// </summary>
internal static class JsonValues
{
    // The form of JavaScript's Date.toISOString(); digits after the milliseconds are dropped.
    private const string DateTimeFormat = "yyyy-MM-dd'T'HH:mm:ss.fff'Z'";

    private static string Format(DateTime utc) => utc.ToString(DateTimeFormat, CultureInfo.InvariantCulture);

    internal sealed class DateTimeConverter : JsonConverter<DateTime>
    {
        public override DateTime Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
            reader.GetDateTime();

        public override void Write(Utf8JsonWriter writer, DateTime value, JsonSerializerOptions options) =>
            writer.WriteStringValue(Format(VexoClient.AsUtc(value)));
    }

    internal sealed class DateTimeOffsetConverter : JsonConverter<DateTimeOffset>
    {
        public override DateTimeOffset Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
            reader.GetDateTimeOffset();

        public override void Write(Utf8JsonWriter writer, DateTimeOffset value, JsonSerializerOptions options) =>
            writer.WriteStringValue(Format(value.UtcDateTime));
    }

    internal sealed class DecimalConverter : JsonConverter<decimal>
    {
        public override decimal Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
            reader.GetDecimal();

        // decimal.ToString() never uses scientific notation, and keeps the scale: 9.90m is "9.90".
        public override void Write(Utf8JsonWriter writer, decimal value, JsonSerializerOptions options) =>
            writer.WriteStringValue(value.ToString(CultureInfo.InvariantCulture));
    }
}
