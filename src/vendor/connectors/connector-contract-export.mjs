const check = (value, message) => {
  if (!value) throw Error(message);
};
const safe = (v) =>
  typeof v === "string" && /^[A-Za-z0-9][A-Za-z0-9_-]{0,100}$/.test(v);
export function connectorContracts(input) {
  check(
    input &&
      Object.keys(input).length === 1 &&
      input.review?.apiVersion === "ingestron.singer-review/v1",
    "Supply one Singer review bundle",
  );
  check(
    input.review.status === "approved",
    "Approve the reviewed projection first",
  );
  const entries = Object.entries(input.review.contracts ?? {});
  check(entries.length >= 1 && entries.length <= 100, "Select 1–100 contracts");
  const artifacts = {};
  for (const [name, contract] of entries) {
    check(
      safe(name) &&
        contract?.apiVersion === "v3.1.0" &&
        typeof contract.id === "string",
      "Invalid ODCS contract identity",
    );
    artifacts[name + ".odcs.json"] = JSON.stringify(contract, null, 2);
  }
  return {
    apiVersion: "ingestron.artifact-proposal/v1",
    applied: false,
    artifacts,
    review: [
      "The host validates ODCS on export. The runtime independently checks agreement with the selected source projection.",
      "Source contracts require deliberate mappings before a common model or report pack can consume them.",
    ],
  };
}
