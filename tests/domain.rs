use quotick::domain::{AccountId, AuctionId, DomainError, Price, Quantity, Side};

#[test]
fn identifiers_reserve_zero_and_preserve_nonzero_values() {
    assert_eq!(
        AccountId::new(0),
        Err(DomainError::ZeroIdentifier("account identifier"))
    );
    assert_eq!(AccountId::new(42).expect("valid identifier").get(), 42);
    assert_eq!(
        AuctionId::new(0),
        Err(DomainError::ZeroIdentifier("auction identifier"))
    );
}

#[test]
fn quantity_is_strictly_positive_but_price_can_be_negative() {
    assert_eq!(Quantity::new(0), Err(DomainError::ZeroQuantity));
    assert_eq!(Quantity::new(7).expect("valid quantity").lots(), 7);
    assert_eq!(Price::from_raw(-37).raw(), -37);
}

#[test]
fn side_opposition_is_an_involution() {
    assert_eq!(Side::Buy.opposite(), Side::Sell);
    assert_eq!(Side::Sell.opposite().opposite(), Side::Sell);
}
