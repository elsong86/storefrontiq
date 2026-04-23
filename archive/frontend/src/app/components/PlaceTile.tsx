import React from 'react';
import { useRouter } from 'next/navigation';
import { Place } from '../types';

interface PlaceTileProps {
  place: Place;
}

const PlaceTile: React.FC<PlaceTileProps> = ({ place }) => {
  const router = useRouter();

  const handleClick = () => {
    const query = new URLSearchParams({
      id: place.id,
      displayName: place.displayName.text,
      formattedAddress: place.formattedAddress,
      rating: place.rating?.toString() || '',
      userRatingCount: place.userRatingCount?.toString() || '',
    }).toString();

    router.push(`/restaurant/${place.id}?${query}`);
  };

  return (
    <div
      onClick={handleClick}
      className="m-2 cursor-pointer rounded border p-4 shadow-lg"
    >
      <h2 className="text-xl font-bold">{place.displayName.text}</h2>
      <p>{place.formattedAddress}</p>
      {place.rating && <p>Rating: {place.rating}</p>}
      {place.userRatingCount && <p>Reviews: {place.userRatingCount}</p>}
    </div>
  );
};

export default PlaceTile;
