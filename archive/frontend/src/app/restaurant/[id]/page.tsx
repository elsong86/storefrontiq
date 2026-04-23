'use client';

import { useSearchParams } from 'next/navigation';
import { useEffect, useState } from 'react';
import { Place } from '../../types';

const apiUrl = process.env.NEXT_PUBLIC_API_URL;

const RestaurantPage: React.FC = () => {
  const searchParams = useSearchParams();
  const [place, setPlace] = useState<Place | null>(null);
  const [reviews, setReviews] = useState<any[]>([]);
  const [averageSentiment, setAverageSentiment] = useState<number | null>(null);
  const [loading, setLoading] = useState<boolean>(true);

  useEffect(() => {
    const id = searchParams.get('id');
    const displayName = searchParams.get('displayName');
    const formattedAddress = searchParams.get('formattedAddress');
    const rating = searchParams.get('rating');
    const userRatingCount = searchParams.get('userRatingCount');

    if (id && displayName && formattedAddress) {
        setPlace({
            id,
            displayName: { text: displayName },
            formattedAddress,
            location: { latitude: 0, longitude: 0 },
            types: [],
            rating: rating ? parseFloat(rating) : undefined,
            userRatingCount: userRatingCount
                ? parseInt(userRatingCount, 10)
                : undefined,
        });

        // Pass displayName and formattedAddress to fetchReviews
        fetchReviews(id, displayName, formattedAddress);
    }
  }, [searchParams]);


  const fetchReviews = async (placeId: string, displayName: string, formattedAddress: string) => {
    setLoading(true);
    try {
        const response = await fetch(
            `${apiUrl}/reviews?place_id=${placeId}&displayName=${encodeURIComponent(displayName)}&formattedAddress=${encodeURIComponent(formattedAddress)}`,
        );
        if (!response.ok) {
            throw new Error(`Error: ${response.statusText}`);
        }
        const data = await response.json();
        console.log('Fetched reviews data:', data);

        if (data.reviews && Array.isArray(data.reviews)) {
            setReviews(data.reviews);
        } else {
            setReviews([]);
        }

        if (data.average_sentiment !== undefined) {
            setAverageSentiment(data.average_sentiment);
        } else {
            setAverageSentiment(null);
        }
      
    } catch (error) {
        console.error('Failed to fetch reviews:', error);
    } finally {
        setLoading(false);
    }
  };


  if (!place) {
    return <div>Loading...</div>;
  }

  return (
    <div className="p-4">
      <h1 className="mb-4 text-3xl font-bold">{place.displayName.text}</h1>
      <p>{place.formattedAddress}</p>
      {place.rating && <p>Rating: {place.rating}</p>}
      {place.userRatingCount && <p>Reviews: {place.userRatingCount}</p>}

      <h2 className="mt-4 text-2xl font-bold">Average Sentiment</h2>
      {averageSentiment !== null ? (
        <p>{averageSentiment.toFixed(2)} / 10</p>
      ) : (
        <p>No sentiment data available.</p>
      )}

      <h2 className="mt-4 text-2xl font-bold">Reviews</h2>
      {loading ? (
        <div>Loading reviews...</div>
      ) : reviews.length > 0 ? (
        <ul>
          {reviews.map((review, index) => (
            <li key={index} className="mb-2">
              <p>- {review.review_text}</p>
            </li>
          ))}
        </ul>
      ) : (
        <div>No reviews available.</div>
      )}
    </div>
  );
};

export default RestaurantPage;
