CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE OR REPLACE FUNCTION token_user_id(
    p_token text,
    p_secret text
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    parts text[];
    signing_input text;
    expected_signature text;
    actual_signature text;
    payload jsonb;
    expires bigint;
BEGIN
    -- Reject NULL or excessively long tokens.
    IF p_token IS NULL OR length(p_token) > 8192 THEN
        RETURN NULL;
    END IF;

    -- Split JWT into its three components.
    parts := string_to_array(p_token, '.');

    IF array_length(parts, 1) IS DISTINCT FROM 3 THEN
        RETURN NULL;
    END IF;

    IF parts[1] = '' OR parts[2] = '' OR parts[3] = '' THEN
        RETURN NULL;
    END IF;

    -- Reconstruct the exact input that Node.js signed.
    signing_input := parts[1] || '.' || parts[2];

    -- Calculate HMAC-SHA256 and convert Base64 to Base64URL.
    expected_signature :=
        rtrim(
            translate(
                replace(
                    encode(
                        hmac(signing_input, p_secret, 'sha256'),
                        'base64'
                    ),
                    E'\n',
                    ''
                ),
                '+/',
                '-_'
            ),
            '='
        );

    -- Verify the signature before trusting the payload.
    actual_signature := parts[3];

    IF actual_signature !~ '^[A-Za-z0-9_-]+$'
       OR actual_signature <> expected_signature THEN
        RETURN NULL;
    END IF;

    -- Decode the Base64URL payload.
    payload := convert_from(
        decode(
            translate(parts[2], '-_', '+/') ||
            repeat(
                '=',
                (4 - length(parts[2]) % 4) % 4
            ),
            'base64'
        ),
        'UTF8'
    )::jsonb;

    -- Validate the expiration claim.
    IF jsonb_typeof(payload -> 'expires') <> 'number' THEN
        RETURN NULL;
    END IF;

    expires := (payload ->> 'expires')::bigint;

   IF floor(extract(epoch FROM clock_timestamp()) ) >= expires THEN
        RETURN NULL;
    END IF;

    -- Return the user ID only if it is a valid integer.
    IF jsonb_typeof(payload -> 'user_id') <> 'number'
       OR (payload ->> 'user_id') !~ '^[0-9]+$' THEN
        RETURN NULL;
    END IF;

    RETURN (payload ->> 'user_id')::integer;

EXCEPTION
    WHEN OTHERS THEN
        -- Malformed Base64, invalid JSON, overflow, etc.
        RETURN NULL;
END;
$$;