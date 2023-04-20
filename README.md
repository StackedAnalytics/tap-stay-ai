# tap-stay-ai

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [Stay AI Open API](https://docs.stay.ai/)
- Extracts the following resources:
  - Subscriptions
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

---

Copyright &copy; 2023 Ryan Gahart