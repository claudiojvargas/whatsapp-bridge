namespace WhatsAppBridge.Api.Models;

public sealed record CreateContactRequest(
    string Name,
    string Number
);
